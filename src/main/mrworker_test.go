package main

import (
	"github.com/stretchr/testify/assert"
	"testing"

	"6.824/mr"
)

func TestWorker(t *testing.T) {
	setup := func() {
		mapf := func(string, string) []mr.KeyValue {
			return nil
		}
		reducef := func(string, []string) string { return "" }
		coor := mr.NewLocalCoordinator()
		localWorker := mr.NewLocalWorker(coor)
		mr.Worker(mapf, reducef)
	}
	fakeCoordinator :=
		t.Run("when worker ask coordinator for jobs", func(t *testing.T) {
			assert.Equal(t, 1, 2, "it should return some jobs")
		})
}
