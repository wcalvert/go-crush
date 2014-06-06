Crush
=====
Crush is a simple library for the Go programming language that persists work into a Redis store so it can be processed in the background.

Installation
------------
First, install Crush from Github:
```
go get https://github.com/wcalvert/go-crush
```

Crush depends on Redis, so do one of the following depending on your platform.

Debian-based distros:
```
apt-get install redis
```

Red Hat-based distros:
```
yum install redis
```

Brew on OS X:
```
brew install redis
```

Usage Example
-------------

```
package main

import "fmt"
import "math/big"
import "time"
import "github.com/wcalvert/go-crush"

type MyService struct{}

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
        c.Add(a, b)
        a = b
        b = c
    }
    fmt.Printf("Fn for n=%d is %d\n", n, a)
}

func main() {
    if err, w := crush.NewWorker(&MyService{}, "MyService", "localhost:6379"); err == nil {
        err = w.Enqueue("Multiply", 1.1, 2.5)
        err = w.Enqueue("Fibonacci", int64(50))
        err = w.Enqueue("Concat", "Hello, ", "World")

        go w.Work()
        time.Sleep(2500 * time.Millisecond)
    } else {
        fmt.Println(err)
    }
}
```

Unit Tests
----------
Unit tests have been writen for Crush, and you can run them like so:
```
go get github.com/bmizerany/assert
cd $GOPATH/github.com/wcalvert/go-crush
go test
```

Note
----
If you need numeric parameters to your methods, it is best to use float64 or int64. Crush will attempt to work with these types, but in the current state of development, no other numeric types are supported. This is something that really needs to be improved.