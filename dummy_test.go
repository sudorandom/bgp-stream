package main

import "fmt"
import "time"

func main() {
	start := time.Now()
	fmt.Println("Dummy check took", time.Since(start))
}
