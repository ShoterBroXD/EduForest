package store

import (
	"context"
	"errors"
	"regexp"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Mongo struct {
	client *mongo.Client
	db     *mongo.Database
}

// NewMongo se conecta y crea los índices. Reintenta unos segundos porque en
// Docker Compose la API puede arrancar antes de que Mongo acepte conexiones.
func NewMongo(ctx context.Context, uri, dbName string) (*Mongo, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(3 * time.Second))
	if err != nil {
		return nil, err
	}
	var pingErr error
	for i := 0; i < 10; i++ {
		if pingErr = client.Ping(ctx, nil); pingErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if pingErr != nil {
		return nil, pingErr
	}
	m := &Mongo{client: client, db: client.Database(dbName)}
	return m, m.indexes(ctx)
}

func (m *Mongo) indexes(ctx context.Context) error {
	idx := map[string][]mongo.IndexModel{
		"metricas_nodos": {
			{Keys: bson.D{{Key: "nodo", Value: 1}, {Key: "fecha", Value: -1}}},
			// Las métricas se borran solas a los 7 días (índice TTL).
			{Keys: bson.D{{Key: "fecha", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(7 * 24 * 3600)},
		},
		"colegios": {
			{Keys: bson.D{{Key: "objetivo", Value: 1}, {Key: "cod_mod", Value: 1}}},
			// Cubre el filtro y el orden completo del ranking, así Mongo no ordena en memoria.
			{Keys: bson.D{{Key: "objetivo", Value: 1}, {Key: "alumnos_alta_esperados", Value: -1}}},
		},
		"entrenamientos": {{Keys: bson.D{{Key: "inicio", Value: -1}}}},
		"logs":           {{Keys: bson.D{{Key: "fecha", Value: -1}}}},
	}
	for coll, models := range idx {
		if _, err := m.db.Collection(coll).Indexes().CreateMany(ctx, models); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mongo) Ping(ctx context.Context) error { return m.client.Ping(ctx, nil) }

func (m *Mongo) Close(ctx context.Context) error { return m.client.Disconnect(ctx) }

func (m *Mongo) GuardarEntrenamiento(ctx context.Context, e *Entrenamiento) error {
	_, err := m.db.Collection("entrenamientos").ReplaceOne(ctx, bson.M{"_id": e.ID}, e, options.Replace().SetUpsert(true))
	return err
}

func (m *Mongo) Entrenamiento(ctx context.Context, id string) (*Entrenamiento, error) {
	var e Entrenamiento
	err := m.db.Collection("entrenamientos").FindOne(ctx, bson.M{"_id": id}).Decode(&e)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrNoEncontrado
	}
	return &e, err
}

func (m *Mongo) Entrenamientos(ctx context.Context, limite int) ([]Entrenamiento, error) {
	opts := options.Find().SetSort(bson.D{{Key: "inicio", Value: -1}}).SetLimit(int64(limite)).
		SetProjection(bson.M{"reportes": 0, "importancia": 0})
	return find[Entrenamiento](ctx, m.db.Collection("entrenamientos"), bson.M{}, opts)
}

func (m *Mongo) CerrarInterrumpidos(ctx context.Context) (int, error) {
	res, err := m.db.Collection("entrenamientos").UpdateMany(ctx, bson.M{"estado": "en_curso"},
		bson.M{"$set": bson.M{"estado": "fallido", "error": ErrInterrumpido, "fin": time.Now()}})
	if err != nil {
		return 0, err
	}
	return int(res.ModifiedCount), nil
}

func (m *Mongo) GuardarMetricas(ctx context.Context, ms []MetricaNodo) error {
	if len(ms) == 0 {
		return nil
	}
	_, err := m.db.Collection("metricas_nodos").InsertMany(ctx, ms)
	return err
}

func (m *Mongo) Metricas(ctx context.Context, nodo string, limite int) ([]MetricaNodo, error) {
	filter := bson.M{}
	if nodo != "" {
		filter["nodo"] = nodo
	}
	opts := options.Find().SetSort(bson.D{{Key: "fecha", Value: -1}}).SetLimit(int64(limite))
	return find[MetricaNodo](ctx, m.db.Collection("metricas_nodos"), filter, opts)
}

// GuardarColegios reemplaza las predicciones de un objetivo por las del
// modelo nuevo.
func (m *Mongo) GuardarColegios(ctx context.Context, objetivo string, cs []Colegio) error {
	coll := m.db.Collection("colegios")
	if _, err := coll.DeleteMany(ctx, bson.M{"objetivo": objetivo}); err != nil {
		return err
	}
	const lote = 5000
	for i := 0; i < len(cs); i += lote {
		end := min(i+lote, len(cs))
		if _, err := coll.InsertMany(ctx, cs[i:end], options.InsertMany().SetOrdered(false)); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mongo) Colegios(ctx context.Context, objetivo string, codMod int) ([]Colegio, error) {
	opts := options.Find().SetSort(bson.D{{Key: "anexo", Value: 1}})
	return find[Colegio](ctx, m.db.Collection("colegios"), bson.M{"objetivo": objetivo, "cod_mod": codMod}, opts)
}

func (m *Mongo) Ranking(ctx context.Context, objetivo string, limite int) ([]Colegio, error) {
	opts := options.Find().SetSort(bson.D{{Key: "alumnos_alta_esperados", Value: -1}}).SetLimit(int64(limite))
	return find[Colegio](ctx, m.db.Collection("colegios"), bson.M{"objetivo": objetivo}, opts)
}

func (m *Mongo) BuscarColegios(ctx context.Context, objetivo, texto string, limite int) ([]Colegio, error) {
	filter := bson.M{"objetivo": objetivo, "nombre": bson.M{"$regex": regexp.QuoteMeta(texto), "$options": "i"}}
	opts := options.Find().SetSort(bson.D{{Key: "alumnos_alta_esperados", Value: -1}}).SetLimit(int64(limite))
	return find[Colegio](ctx, m.db.Collection("colegios"), filter, opts)
}

func (m *Mongo) Usuarios(ctx context.Context) ([]Usuario, error) {
	return find[Usuario](ctx, m.db.Collection("usuarios"), bson.M{}, options.Find().SetSort(bson.D{{Key: "creado", Value: 1}}))
}

func (m *Mongo) Usuario(ctx context.Context, nombre string) (*Usuario, error) {
	var u Usuario
	err := m.db.Collection("usuarios").FindOne(ctx, bson.M{"_id": nombre}).Decode(&u)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrNoEncontrado
	}
	return &u, err
}

func (m *Mongo) GuardarUsuario(ctx context.Context, u *Usuario) error {
	_, err := m.db.Collection("usuarios").ReplaceOne(ctx, bson.M{"_id": u.Nombre}, u, options.Replace().SetUpsert(true))
	return err
}

func (m *Mongo) GuardarLog(ctx context.Context, l Log) error {
	_, err := m.db.Collection("logs").InsertOne(ctx, l)
	return err
}

func (m *Mongo) Logs(ctx context.Context, limite int) ([]Log, error) {
	opts := options.Find().SetSort(bson.D{{Key: "fecha", Value: -1}}).SetLimit(int64(limite)).SetProjection(bson.M{"_id": 0})
	return find[Log](ctx, m.db.Collection("logs"), bson.M{}, opts)
}

func find[T any](ctx context.Context, coll *mongo.Collection, filter any, opts *options.FindOptionsBuilder) ([]T, error) {
	cur, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	out := []T{}
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}
