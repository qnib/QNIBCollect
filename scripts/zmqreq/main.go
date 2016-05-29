package main

import (
    "log"
    "os"
    "fmt"
    "fullerite/metric"
    zmq "github.com/pebbe/zmq4"
)

func main() {
    // Create a dealer socket and connect it to the ZmqPUB socket.
    socket, err := zmq.NewSocket(zmq.REQ)
    if err != nil {
        log.Fatal("Socket-Err: ", err)
    }
    socket.Connect(os.Args[1])
    defer socket.Close()

    log.Println("Subscriber created and connected")

    // send filter
    good := map[string]string{}
    f := metric.NewFilter(os.Args[2], "gauge", good)
	msg := f.ToJSON()
	fmt.Println("Sending ", msg)
	socket.Send(string(msg), 0)
	// Wait for replies:
    for {
        reply, _ := socket.Recv(0)
        if reply == "EOM" {
            break
        } else {
            fmt.Println("Received ", reply)
        }
    }
}
