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

type ConsumerService struct {
    conn            *kafka.Conn
    applicationURL  string
}

func New(kafkaURL string, topic string, partition int, applicationURL string) (*ConsumerService, error) {
    var err error
    s := ConsumerService{}
    s.conn, err = kafka.DialLeader(context.Background(), "tcp", kafkaURL, topic, partition)
    if err != nil {
        return nil, err
    }
    s.applicationURL = applicationURL
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
    if err != nil {
        return
    }
    var msg Msg
    if err := json.Unmarshal(message.Value, &msg); err != nil {
        log.Printf(`failed to unmarshal message: {%s}`, err)
        return
    }
    if msg.Id == nil || msg.Username == nil || msg.Time == nil || msg.Text == nil || msg.Error == nil {
        log.Printf(`missing required field in message: {%s}`, msg)
        return
    }
    if err := s.Receive(msg); err != nil {
        log.Printf(`failed to send message: {%s}`, err)
        return
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
