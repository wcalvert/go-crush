package crush 

import "fmt"
import "math/big"
import "math/cmplx"
import "testing"
import "time"
import "github.com/bmizerany/assert"

type MyService struct {}

func (myService *MyService) Multiply(i float64, j float64) {
	fmt.Printf("i*j is %2.2f\n", i*j)
}

func (myService *MyService) Concat(a string, b string) {
	fmt.Printf("%s%s\n", a, b)
}

func (myService *MyService) Fibonacci(i int64) {
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
	panic("This is a panic!")
}

func TestValidArgs(t *testing.T) {
	err, w := NewWorker(&MyService{}, "MyService", "localhost:6379")
	assert.Equal(t, err, nil)

	err = w.Enqueue("Multiply", 1.1, 2.5)
	assert.Equal(t, err, nil)
	err = w.Enqueue("Fibonacci", int64(50))
	assert.Equal(t, err, nil)
	err = w.Enqueue("Multiply", 1.2, 2.6)
	assert.Equal(t, err, nil)
	err = w.Enqueue("Fibonacci", int64(100))
	assert.Equal(t, err, nil)
	err = w.Enqueue("Concat", "hello, ", "world")

	go w.Work()
	time.Sleep(2500 * time.Millisecond)
}

func TestPanic(t *testing.T) {
	err, w := NewWorker(&MyService{}, "MyService", "localhost:6379")
	assert.Equal(t, err, nil)

	err = w.Enqueue("Explode")
	assert.Equal(t, err, nil)
}

func TestInvalidNumArgs(t *testing.T) {
	err, w := NewWorker(&MyService{}, "MyService", "localhost:6379")
	assert.Equal(t, err, nil)

	err = w.Enqueue("Multiply")
	assert.NotEqual(t, err, nil)

	err = w.Enqueue("Multiply", 1.1, 2.2, 3.3)
	assert.NotEqual(t, err, nil)
}

func TestInvalidTypeArgs(t *testing.T) {
	err, w := NewWorker(&MyService{}, "MyService", "localhost:6379")
	assert.Equal(t, err, nil)

	err = w.Enqueue("Multiply", "asdf", "qwerty")
	assert.NotEqual(t, err, nil)

	err = w.Enqueue("Multiply", true, true)
	assert.NotEqual(t, err, nil)

	err = w.Enqueue("Multiply", 1, 2)
	assert.NotEqual(t, err, nil)

	err = w.Enqueue("Multiply", cmplx.Sqrt(-5 + 12i), cmplx.Sqrt(-7 + 4i))
	assert.NotEqual(t, err, nil)

	err = w.Enqueue("Multiply", big.NewInt(1), big.NewInt(2))
	assert.NotEqual(t, err, nil)

	err = w.Enqueue("Fibonacci", 12)
	assert.NotEqual(t, err, nil)

	err = w.Enqueue("Fibonacci", 12.0)
	assert.NotEqual(t, err, nil)

	err = w.Enqueue("Fibonacci", true)
	assert.NotEqual(t, err, nil)

	err = w.Enqueue("Fibonacci", "asdf")
	assert.NotEqual(t, err, nil)
}

func TestInvalidMethod(t *testing.T) {
	err, w := NewWorker(&MyService{}, "MyService", "localhost:6379")
	assert.Equal(t, err, nil)

	err = w.Enqueue("Derp")
	assert.NotEqual(t, err, nil)
}

func TestIncorrectRedisHost(t *testing.T) {
	err, _ := NewWorker(&MyService{}, "MyService", "localhost:9999")
	assert.NotEqual(t, err, nil)
}