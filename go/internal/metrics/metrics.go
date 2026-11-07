package metrics

import (
	"fmt"
	"strings"
)

type Report struct {
	Confusion   [][]int // Confusion[real][predicha]
	Precision   []float64
	Recall      []float64
	F1          []float64
	Support     []int
	Accuracy    float64
	MacroF1     float64
	BalancedAcc float64 // promedio del recall de cada clase
}

func Evaluate(yTrue, yPred []int, numClasses int) Report {
	r := Report{
		Confusion: make([][]int, numClasses),
		Precision: make([]float64, numClasses),
		Recall:    make([]float64, numClasses),
		F1:        make([]float64, numClasses),
		Support:   make([]int, numClasses),
	}
	for c := range r.Confusion {
		r.Confusion[c] = make([]int, numClasses)
	}
	correct := 0
	for i := range yTrue {
		r.Confusion[yTrue[i]][yPred[i]]++
		if yTrue[i] == yPred[i] {
			correct++
		}
	}
	if len(yTrue) > 0 {
		r.Accuracy = float64(correct) / float64(len(yTrue))
	}
	for c := 0; c < numClasses; c++ {
		tp := r.Confusion[c][c]
		predicted, actual := 0, 0
		for o := 0; o < numClasses; o++ {
			predicted += r.Confusion[o][c]
			actual += r.Confusion[c][o]
		}
		r.Support[c] = actual
		if predicted > 0 {
			r.Precision[c] = float64(tp) / float64(predicted)
		}
		if actual > 0 {
			r.Recall[c] = float64(tp) / float64(actual)
		}
		if p, rc := r.Precision[c], r.Recall[c]; p+rc > 0 {
			r.F1[c] = 2 * p * rc / (p + rc)
		}
		r.MacroF1 += r.F1[c] / float64(numClasses)
		r.BalancedAcc += r.Recall[c] / float64(numClasses)
	}
	return r
}

func (r Report) String(classNames []string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%-14s %9s %9s %9s %9s\n", "Clase", "Precisión", "Recall", "F1", "Soporte")
	for c, name := range classNames {
		fmt.Fprintf(&sb, "%-14s %9.3f %9.3f %9.3f %9d\n", name, r.Precision[c], r.Recall[c], r.F1[c], r.Support[c])
	}
	fmt.Fprintf(&sb, "\nExactitud: %.3f | Exactitud balanceada: %.3f | F1 macro: %.3f\n", r.Accuracy, r.BalancedAcc, r.MacroF1)
	fmt.Fprintf(&sb, "\nMatriz de confusión (filas = real, columnas = predicha):\n%-14s", "")
	for _, name := range classNames {
		fmt.Fprintf(&sb, " %13s", name)
	}
	sb.WriteString("\n")
	for c, name := range classNames {
		fmt.Fprintf(&sb, "%-14s", name)
		for o := range classNames {
			fmt.Fprintf(&sb, " %13d", r.Confusion[c][o])
		}
		sb.WriteString("\n")
	}
	return sb.String()
}
