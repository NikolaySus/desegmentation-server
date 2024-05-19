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
    "syscall"
    "time"
    kafka "github.com/segmentio/kafka-go"
)

type Msg struct {
    Id          *string `json:"id"`
    Username    *string `json:"username"`
    Time        *string `json:"time"`
    Text        *string `json:"text"`
    Error       *string `json:"error"`
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

func (s *ConsumerService) doJob() {
    message, err := s.conn.ReadMessage(1e3)
    if err == nil {
        var sgt Sgt
        if err := json.Unmarshal(message.Value, &sgt); err != nil {
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
            var str string
            for i := 0; i < *sgt.SegmentsCount; i++ {
                str += *s.segments[*sgt.Time][i].Payload
            }
            delete(s.segments, *sgt.Time)
            var msg Msg
            if err := json.Unmarshal([]byte(str), &msg); err != nil {
                log.Printf(`failed to unmarshal message: {%s}`, err)
                return
            }
            msgError := "OK"
            msg.Error = &msgError
            if msg.Id == nil || msg.Username == nil || msg.Time == nil || msg.Text == nil || msg.Error == nil {
                log.Printf(`missing required field in message: {%s}`, msg)
                return
            }
            if err := s.Receive(msg); err != nil {
                log.Printf(`failed to send message: {%s}`, err)
                //return
            }
        }
    }
    AGGR_TIMEOUT_SECONDS, _ := time.ParseDuration("1s")
    for _, v := range s.segments {
        t, err := time.Parse(time.RFC3339Nano, *v[0].Time)
        if err == nil {
            log.Print("is less than 1s: ", time.Now().Sub(t) < AGGR_TIMEOUT_SECONDS)
        }
    }
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
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        for {
            select {
            case <-quit:
                return
            default:
                s.doJob()
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
