package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
)

func sendMessageHandler(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")
	queue, ok := queues[queueName]
	if !ok {
		http.Error(w, "Incorrect queue name", http.StatusBadRequest)
		return
	}

	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	capacityExceeded := func() bool {
		if len(queue.ch) == queue.capacity {
			return true
		}

		queue.messagesMutex.Lock()
		defer queue.messagesMutex.Unlock()

		if len(queue.ch) == queue.capacity {
			return true
		}

		fmt.Printf("received message: %s\n", string(bytes))

		queue.ch <- bytes
		return false
	}()

	if capacityExceeded {
		http.Error(w, "Queue capacity was exceeded", http.StatusBadRequest)
	}
}

func createSubscriptionHandler(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")
	queue, ok := queues[queueName]
	if !ok {
		http.Error(w, "Incorrect queue name", http.StatusBadRequest)
		return
	}

	subsrCh := make(chan []byte)

	limitExceeded := func() bool {
		if len(queue.subscriptions) == queue.maxSubscriptions {
			return true
		}

		queue.mutex.Lock()
		defer queue.mutex.Unlock()

		if len(queue.subscriptions) == queue.maxSubscriptions {
			return true
		}

		queue.subscriptions = append(queue.subscriptions, subsrCh)
		// Если это первый подписчик, отправляем сигнал, что можно больше не копить элементы в очереди
		if len(queue.subscriptions) == 1 {
			close(*queue.noSubscriptionSignal)
		}

		return false
	}()

	if limitExceeded {
		http.Error(w, "Subscriptions queue limit was exceeded", http.StatusBadRequest)
		return
	}

	fmt.Printf("subscribed to %s\n", queueName)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	for {
		select {
		case msg := <-subsrCh:
			_, err := w.Write(msg)
			if err != nil {
				log.Println(err)
				http.Error(w, "Internal error", http.StatusInternalServerError)
				return
			}
			w.(http.Flusher).Flush()
		case <-r.Context().Done():
			// Отписка
			queue.mutex.Lock()
			var index int
			for i, s := range queue.subscriptions {
				if s == subsrCh {
					index = i
					break
				}
			}
			queue.subscriptions = append(queue.subscriptions[:index], queue.subscriptions[index+1:]...)
			if len(queue.subscriptions) == 0 {
				noSubscriptionSignal := make(chan struct{})
				queue.noSubscriptionSignal = &noSubscriptionSignal
			}
			queue.mutex.Unlock()
			fmt.Printf("unsubscribed from %s\n", queueName)
			return
		}
	}
}

// учесть максимальное число подписчиков - если оно достигнуто, то метод subscriptions вернет ошибку
// учесть максимальное число элементов очереди - если оно достигнуто, то метод messages вернет ошибку
// если нет подписчиков, то ждать их и копить сообщения
// удалить сообщение, когда оно всеми прочитано

func broadcast() {
	for qName, q := range queues {
		go func() {
			for msg := range q.ch {
				<-*q.noSubscriptionSignal
				// К этому моменту noSubscriptionSignal может быть присвоен новый канал, потому что отписался последний подписчик и тогда мы не отправим msg ни одному подписчику, а просто потеряем его
				// Но кажется что вероятность этого стремится к нулю, так как канал закрывается под мьютексом. Чтобы этот кейс произошел,
				// мьютекс отписки должен успеть захватиться сразу после подписки минуя мьютекс чтения ниже - там до него еще есть код, поэтому защита здесь не предусмотрена.
				q.mutex.Lock()
				for _, s := range q.subscriptions {
					fmt.Printf("send message to %s\n", qName)
					s <- msg
				}
				q.mutex.Unlock()
			}
		}()
	}
}

type Queue struct {
	capacity             int
	maxSubscriptions     int
	ch                   chan []byte
	subscriptions        []chan []byte
	mutex                *sync.Mutex
	noSubscriptionSignal *chan struct{}
	messagesMutex        *sync.Mutex
}

func NewQueue(capacity int, maxSubscriptions int) *Queue {
	noSubscriptionSignal := make(chan struct{})
	return &Queue{
		capacity:             capacity,
		maxSubscriptions:     maxSubscriptions,
		ch:                   make(chan []byte, capacity),
		subscriptions:        make([]chan []byte, 0, maxSubscriptions),
		mutex:                &sync.Mutex{},
		noSubscriptionSignal: &noSubscriptionSignal,
		messagesMutex:        &sync.Mutex{},
	}
}

var queues map[string]*Queue

func main() {
	queues = map[string]*Queue{
		"q10":   NewQueue(10, 2),
		"q1":    NewQueue(1, 3),
		"q1000": NewQueue(1000, 10),
		"q2":    NewQueue(2, 1),
	}
	_ = queues

	mux := http.NewServeMux()

	s := &http.Server{
		Addr:    ":5600",
		Handler: mux,
	}

	mux.Handle("POST /v1/queues/{queueName}/messages", http.HandlerFunc(sendMessageHandler))
	mux.Handle("POST /v1/queues/{queueName}/subscriptions", http.HandlerFunc(createSubscriptionHandler))

	broadcast()

	if err := s.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
