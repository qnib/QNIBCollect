package main

import (
    "log"
    "os"
    "time"

    zmq "github.com/pebbe/zmq4"
)

func main() {
    // Create a PUB socket
    publisher, _ := zmq.NewSocket(zmq.PUB)
	  publisher.Bind(os.Args[1])
    defer publisher.Close()

    log.Println("Publisher created and bound")

    for {
        log.Println("sending two messages")
        //  Write two messages, each with an envelope and content
        publisher.Send("A", zmq.SNDMORE)
        publisher.Send("We don't want to see this", 0)
        publisher.Send("B", zmq.SNDMORE)
        publisher.Send("We would like to see this", 0)
        time.Sleep(time.Second)
    }

}
