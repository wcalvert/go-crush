package crush

import "github.com/hoisie/redis"
import "encoding/json"
import "reflect"
import "errors"
import "time"
import "fmt"
import "net/http"

const QueuePrefix = "crush:queued:"
const QueueFinishedPrefix = "crush:finished:"
const QueueFailedPrefix = "crush:failed:"

type Worker struct {
    service interface{}
    WorkQueue string
    FinishedQueue string
    FailedQueue string
    client redis.Client
    IdleSleep int

}

func NewWorker(service interface{}, name string) (*Worker) {
    w := new(Worker)
    w.IdleSleep = 1000
    w.service = service
    w.WorkQueue = QueuePrefix + name
    w.FinishedQueue = QueueFinishedPrefix + name
    w.FailedQueue = QueueFailedPrefix +name
    return w
}

type QueuedCall struct {
    MethodName string
    Args []interface{}
    Enqueued int64
    Executed int64
}

func (w *Worker) EnqueueFailed(qc QueuedCall) {
    if b, err := json.Marshal(qc); err == nil {
        w.client.Rpush(w.FailedQueue, b)
    }
}

func (w *Worker) EnqueueFinished(qc QueuedCall) {
    if b, err := json.Marshal(qc); err == nil {
        w.client.Rpush(w.FinishedQueue, b)
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
    if err = w.SanityCheck(qc); err != nil {
        return err
    }
    if b, err := json.Marshal(qc); err != nil {
        return err
    } else {
        w.client.Rpush(w.WorkQueue, b)
    }
    return err
}

func (w *Worker) Dequeue() (QueuedCall, error) {
    var qc QueuedCall

    b, err := w.client.Lpop(w.WorkQueue)
    if err != nil {
        return qc, err
    }

    err = json.Unmarshal(b, &qc)
    if err != nil {
        return qc, err
    }

    err = w.SanityCheck(qc)
    return qc, err
    
}

func (w *Worker) Work() {
    for {
        if outstanding, err := w.client.Llen(w.WorkQueue); err == nil {
            if outstanding > 0 {
                if qc, err := w.Dequeue(); err == nil {
                    if err = w.Invoke(qc); err != nil {
                        fmt.Println(err)
                        w.EnqueueFailed(qc)
                    } else {
                        qc.Executed = time.Now().Unix()
                        w.EnqueueFinished(qc)
                    }
                } else {
                    fmt.Println(err)
                }
            } else {
                time.Sleep(time.Duration(w.IdleSleep) * time.Millisecond)
            }
        } else {
            fmt.Println(err)
        }
    }
}

func (w *Worker) SanityCheck(qc QueuedCall) (error) {

    if !reflect.ValueOf(w.service).MethodByName(qc.MethodName).IsValid() {
        err := errors.New("Invalid method")
        return err
    }

    if len(qc.Args) != reflect.ValueOf(w.service).MethodByName(qc.MethodName).Type().NumIn() {
        err := errors.New("Incorrect number of arguments")
        return err
    }

    //TODO: add a check for correct argument type
    
    return nil
}

func (w *Worker) Invoke(qc QueuedCall) (err error) {

    if err = w.SanityCheck(qc); err != nil {
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

func RootHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "TODO: Add stuff here.")
}

func (w *Worker) ServeHttp(address string) {
    http.HandleFunc("/", RootHandler)
    http.Handle("/static", http.FileServer(http.Dir("./static/")))
    if err := http.ListenAndServe(address, nil); err != nil {
        panic(err)
    }
}