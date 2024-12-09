package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	client := http.Client{}
	buf := make([]byte, 0)
	reader := bytes.NewReader(buf)
	resp, err := client.Post("http://localhost:5600/v1/queues/q10/subscriptions", "text/event-stream", reader)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
}
