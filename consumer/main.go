package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "os/signal"
    "sort"
    "syscall"
    "time"
    kafka "github.com/segmentio/kafka-go"
)

type Msg struct {
    Data    *string `json:"data"`
    Error   *string `json:"error"`
}

type Sgt struct {
    Payload         *string `json:"payload"`
    Time            *string `json:"time"`
    SegmentsCount   *int    `json:"segments_count"`
    SegmentNum      *int    `json:"segment_num"`
}

type ConsumerService struct {
    conn            *kafka.Conn
    applicationURL  string
    segments        map[string]map[int]Sgt
}

func New(kafkaURL string, topic string, partition int, applicationURL string) (*ConsumerService, error) {
    var err error
    s := ConsumerService{}
    s.conn, err = kafka.DialLeader(context.Background(), "tcp", kafkaURL, topic, partition)
    if err != nil {
        return nil, err
    }
    s.applicationURL = applicationURL
    s.segments = make(map[string]map[int]Sgt)
    return &s, nil
}

func (s *ConsumerService) Receive(message Msg) error {
    payload, err := json.Marshal(message)
    if err != nil {
        return fmt.Errorf(`failed to marshal message: {%s}`, err)
    }
    log.Print("sending json: ", string(payload))
    resp, err := http.Post(s.applicationURL + "/receive", "application/json", bytes.NewReader(payload))
    if err != nil {
        return fmt.Errorf(`application service unavailable: {%s}`, err)
    }
    if resp.StatusCode >= 300 {
        return fmt.Errorf(`application service failed: {%s}`, resp.Status)
    }
    return nil
}

func (s *ConsumerService) BuildReceive(pool map[int]Sgt, status string) {
    var str string
    keys := make([]int, 0)
    for k, _ := range pool {
        keys = append(keys, k)
    }
    sort.Ints(keys)
    for _, k := range keys {
        str += *pool[k].Payload
    }
    var msg Msg
    msg.Data = &str
    msg.Error = &status
    if err := s.Receive(msg); err != nil {
        log.Printf(`failed to send message: {%s}`, err)
    }
}

func GetSomeKey(m map[int]Sgt) int {
    for k := range m {
        return k
    }
    return 0
}

func (s *ConsumerService) Check() {
    AGGR_TIMEOUT_SECONDS, _ := time.ParseDuration("1s")
    for k := range s.segments {
        t, err := time.Parse(time.RFC3339Nano, *s.segments[k][GetSomeKey(s.segments[k])].Time)
        if err == nil {
            if (time.Now().Sub(t) > AGGR_TIMEOUT_SECONDS) {
                log.Print(k, " reached 1s timeout, sending with error")
                s.BuildReceive(s.segments[k], "MISSING_SEGMENTS")
                delete(s.segments, k)
            }
        } else {
            log.Print("ALARM!!! UNEXPECTED ERROR!!!")
            return
        }
    }
}

func (s *ConsumerService) ReadMessage() kafka.Message {
    msg, _ := s.conn.ReadMessage(1e4)
    return msg;
}

func main() {
    kafkaURL := os.Getenv("kafkaURL")
    topic := os.Getenv("topic")
    port := os.Getenv("port")
    applicationURL := "http://" + os.Getenv("applicationURL")
    for i := 3; i > 0; i-- {
        log.Print("consumer sleeping for ", i, " seconds")
        time.Sleep(1 * time.Second)
    }
    log.Print("consumer starting at :", port, " with kafkaURL=", kafkaURL, ", topic=", topic, " and applicationURL=", applicationURL)
    partition := 0
    s, err := New(kafkaURL, topic, partition, applicationURL)
    if err != nil {
        log.Fatal("failed to dial leader:", err)
        return
    }
    readMessage := make(chan kafka.Message)
    go func() {
       for msg := s.ReadMessage(); true; msg = s.ReadMessage() {
           readMessage <- msg
       }
    }()
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        for {
            select {
            case <-quit:
                return
            case segment := <-readMessage:
                var sgt Sgt
                if err := json.Unmarshal(segment.Value, &sgt); err != nil {
                    log.Printf(`failed to unmarshal segment: {%s}`, err)
                    return
                }
                if sgt.Payload == nil || sgt.Time == nil || sgt.SegmentsCount == nil || sgt.SegmentNum == nil {
                    log.Printf(`missing required field in segment: {%s}`, sgt)
                    return
                }
                if _, ok := s.segments[*sgt.Time]; !ok {
                    s.segments[*sgt.Time] = make(map[int]Sgt)
                }
                s.segments[*sgt.Time][*sgt.SegmentNum] = sgt
                if len(s.segments[*sgt.Time]) == *sgt.SegmentsCount {
                    s.BuildReceive(s.segments[*sgt.Time], "OK")
                    delete(s.segments, *sgt.Time)
                }
            default:
                s.Check()
            }
        }
    }()
    <-quit
    log.Println("shutdown consumer service ...")
    s.conn.DeleteTopics(topic)
    if err := s.conn.Close(); err != nil {
        log.Fatal("failed to close reader:", err)
    }
    ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Second)
    defer cancel()
    select {
    case <-ctx.Done():
        log.Println("shutdown timeout has expired")
    }
    log.Println("consumer service exiting")
}
