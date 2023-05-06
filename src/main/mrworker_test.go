package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWorker(t *testing.T) {
	t.Run("when worker ask coordinator for jobs", func(t *testing.T) {
		assert.Equal(t, 1, 2, "it should return some jobs")
	})
}
