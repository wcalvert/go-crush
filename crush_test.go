package main

import "fmt"
import "math/big"
import "crush"

type MyService struct {}

func (myService *MyService) Multiply(i float64, j float64) {
	fmt.Printf("i*j is %2.2f\n", i*j)
}

func (myService *MyService) Fibonacci(i float64) {
	n := int(i)
	a := big.NewInt(0)
	b := big.NewInt(1)
	for i := 0; i < n; i++ {
		c := new(big.Int)
		c.Add(a,b)
		a = b
		b = c
	}
	fmt.Printf("Fn for n=%d is %d\n", n, a)
}

func (myService *MyService) Explode() {
	panic("Something went horribly wrong!")
}

func main() {
	w := crush.NewWorker(&MyService{}, "MyService")

	go w.ServeHttp("0.0.0.0:8080")

	w.Enqueue("Multiply", 1.1, 2.3)
	w.Enqueue("Fibonacci", 10)
	w.Enqueue("Multiply", 1.1, 2.4)
	w.Enqueue("Fibonacci", 20)
	w.Enqueue("Multiply", 1.1, 2.5)
	w.Enqueue("Fibonacci", 30)
	w.Enqueue("Multiply", 1.1, 2.6)
	w.Enqueue("Fibonacci", 40)
	w.Enqueue("Explode")

	w.Work()
}