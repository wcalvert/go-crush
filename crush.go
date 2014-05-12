package crush

import "github.com/hoisie/redis"
import "encoding/json"
import "reflect"
import "errors"
import "time"
import "fmt"
import "net/http"
import "strings"

const QueuePrefix = "crush:queued:"
const QueueFinishedPrefix = "crush:finished:"
const QueueFailedPrefix = "crush:failed:"

type Worker struct {
    service interface{}
    workQueue string
    finishedQueue string
    failedQueue string
    client redis.Client
    idleSleep int

}

func NewWorker(service interface{}, name string, addr string) (error, *Worker) {
    w := new(Worker)
    w.idleSleep = 1000
    w.service = service
    w.workQueue = QueuePrefix + name
    w.finishedQueue = QueueFinishedPrefix + name
    w.failedQueue = QueueFailedPrefix +name
    w.client.Addr = addr
    return w.client.Flush(true), w
}

type QueuedCall struct {
    MethodName string
    Args []interface{}
    Enqueued int64
    Executed int64
}

func (w *Worker) enqueueFailed(qc QueuedCall) {
    if b, err := json.Marshal(qc); err == nil {
        w.client.Rpush(w.failedQueue, b)
    }
}

func (w *Worker) enqueueFinished(qc QueuedCall) {
    if b, err := json.Marshal(qc); err == nil {
        w.client.Rpush(w.finishedQueue, b)
    }
}

func (w *Worker) Enqueue(name string, args... interface{}) (err error) {
    var qc QueuedCall
    qc.MethodName = name
    qc.Args = make([]interface{}, len(args))
    qc.Enqueued = time.Now().Unix()
    for i, param := range args {
        qc.Args[i] = param
    }
    if err = w.sanityCheck(qc); err != nil {
        return err
    }
    if b, err := json.Marshal(qc); err != nil {
        return err
    } else {
        w.client.Rpush(w.workQueue, b)
    }
    return err
}

func (w *Worker) dequeue() (QueuedCall, error) {
    var qc QueuedCall

    b, err := w.client.Lpop(w.workQueue)
    if err != nil {
        return qc, err
    }

    d := json.NewDecoder(strings.NewReader(string(b)))
    d.UseNumber()
    if err = d.Decode(&qc); err != nil {
        return qc, err
    }

    for i, param := range qc.Args {
        switch param.(type) {
        case json.Number:
            if strings.Index(param.(json.Number).String(), ".") != -1 {
                if f64, conv_err := param.(json.Number).Float64(); conv_err == nil {
                    qc.Args[i] = f64
                }
            } else {
                if i64, conv_err := param.(json.Number).Int64(); conv_err == nil {
                    qc.Args[i] = i64
                }
            }
        }
    }

    err = w.sanityCheck(qc)
    return qc, err
}

func (w *Worker) Work() {
    for {
        if outstanding, err := w.client.Llen(w.workQueue); err == nil {
            if outstanding > 0 {
                if qc, err := w.dequeue(); err == nil {
                    if err = w.invoke(qc); err != nil {
                        fmt.Println(err)
                        w.enqueueFailed(qc)
                    } else {
                        qc.Executed = time.Now().Unix()
                        w.enqueueFinished(qc)
                    }
                } else {
                    fmt.Println(err)
                }
            } else {
                time.Sleep(time.Duration(w.idleSleep) * time.Millisecond)
            }
        } else {
            fmt.Println(err)
        }
    }
}

func (w *Worker) sanityCheck(qc QueuedCall) (err error) {

    if !reflect.ValueOf(w.service).MethodByName(qc.MethodName).IsValid() {
        err = errors.New("Invalid method")
        return err
    }

    if len(qc.Args) != reflect.ValueOf(w.service).MethodByName(qc.MethodName).Type().NumIn() {
        err = errors.New("Incorrect number of arguments")
        return err
    }

    for i, param := range qc.Args {
        if reflect.ValueOf(w.service).MethodByName(qc.MethodName).Type().In(i) != reflect.ValueOf(param).Type() {
            err = errors.New("Argument type mismatch")
            return err
        }
    }

    return err
}

func (w *Worker) invoke(qc QueuedCall) (err error) {

    if err = w.sanityCheck(qc); err != nil {
        return err
    }

    inputs := make([]reflect.Value, len(qc.Args))
    for i, param := range qc.Args {
        inputs[i] = reflect.ValueOf(param)
    }

    defer func() {
        if r := recover(); r != nil {
            switch t := r.(type) {
            case string:
                err = errors.New(t)
                break;
            case error:
                err = t
                break;
            default:
                err = errors.New("Unknown panic")
            }
        }
    }()

    reflect.ValueOf(w.service).MethodByName(qc.MethodName).Call(inputs)

    return err
}

func rootHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "TODO: Add stuff here.")
}

func (w *Worker) ServeHttp(address string) {
    http.HandleFunc("/", rootHandler)
    http.Handle("/static", http.FileServer(http.Dir("./static/")))
    if err := http.ListenAndServe(address, nil); err != nil {
        panic(err)
    }
}