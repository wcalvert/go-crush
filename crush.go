package crush

import "github.com/hoisie/redis"
import "encoding/json"
import "reflect"
import "errors"
import "time"

const QueuePrefix = "crush:queues:"

type Worker struct {
    service interface{}
    WorkQueue string
    client redis.Client
    idleSleep int
}

func NewWorker(service interface{}, name string) (*Worker) {
    w := new(Worker)
    w.service = service
    w.WorkQueue = QueuePrefix + name
    return w
}

type QueuedCall struct {
    MethodName string
    Args []interface{}
}

func (w *Worker) Enqueue(name string, args... interface{}) (error) {
    var qc QueuedCall
    qc.MethodName = name
    qc.Args = make([]interface{}, len(args))
    for i, param := range args {
        qc.Args[i] = param
    }
    if err := SanityCheck(w.service, qc); err != nil {
        return err
    }
    b, err2 := json.Marshal(qc); 
    if err2 != nil {
        return err2
    }
    w.client.Rpush(w.WorkQueue, b)
    return nil
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

    err = SanityCheck(w.service, qc)
    return qc, err
    
}

func (w *Worker) Work() {
    for {
        if outstanding, err := w.client.Llen(w.WorkQueue); err == nil {
            if outstanding > 0 {
                if qc, err := w.Dequeue(); err == nil {
                    if err = w.Invoke(qc); err != nil {
                        // do some kind of error thing
                    }
                } else {
                    // do some kind of error thing
                }
            } else {
                time.Sleep(time.Duration(1000 * time.Millisecond))
            }
        } else {
            // do some kind of error thing here
        }
    }
}

func SanityCheck(service interface{}, qc QueuedCall) (error) {

    if !reflect.ValueOf(service).MethodByName(qc.MethodName).IsValid() {
        err := errors.New("Invalid method")
        return err
    }

    if len(qc.Args) != reflect.ValueOf(service).MethodByName(qc.MethodName).Type().NumIn() {
        err := errors.New("Incorrect number of arguments")
        return err
    }
    return nil
}

func (w *Worker) Invoke(qc QueuedCall) (error) {

    if err := SanityCheck(w.service, qc); err != nil {
        return err
    }

    inputs := make([]reflect.Value, len(qc.Args))
    for i, param := range qc.Args {
        inputs[i] = reflect.ValueOf(param)
    }
    
    go reflect.ValueOf(w.service).MethodByName(qc.MethodName).Call(inputs)

    return nil
}
