package main

import (
	"context"
	"testing"

	"6.824/mr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestWorker(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Worker")
}

var _ = Describe("LocalWorker", func() {
	var (
		localWorker mr.Worker
		coor        *mr.Coordinator
	)
	BeforeEach(func() {
		mapf := func(string, string) []mr.KeyValue {
			return nil
		}
		reducef := func(string, []string) string { return "" }
		coor = mr.NewLocalCoordinator()
		localWorker = mr.NewLocalWorker(coor.MailBox, mapf, reducef)
		go localWorker.Serve(context.Background())
	})

	AfterEach(func() {
		localWorker.Shutdown()
	})

	When("worker ask coordinator for jobs", func() {
		var (
			req   *mr.WordCountArgs
			reply *mr.WordCountReply
			err   error
		)
		BeforeEach(func() {
			req = &mr.WordCountArgs{FileNames: []string{"pg-being_ernest.txt", "pg-grimm.txt"}}
			reply = &mr.WordCountReply{}
			err = coor.WordCount(req, reply)
			Expect(err).ShouldNot(HaveOccurred())
			coor.Wait()
		})
		It("should return correct jobs", func() {
			Expect(err).ShouldNot(HaveOccurred())
			Expect(req.X + 1).To(Equal(reply.Y))
		})
	})
})
