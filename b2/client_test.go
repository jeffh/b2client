package b2

import (
	"bytes"
	"testing"
)

func TestListingBuckets(t *testing.T) {
	c, ok := liveTestRetryClient(t, true)
	if !ok {
		return
	}

	resp, err := c.ListBuckets(&ListBucketsOptions{BucketName: integrationConfig.BucketName})
	if err != nil {
		t.Fatalf("Failed to list buckets: %s", err)
	}

	if !(len(resp.Buckets) > 0) {
		found := false
		for _, bkt := range resp.Buckets {
			if bkt.BucketID == integrationConfig.BucketID {
				found = bkt.BucketName == integrationConfig.BucketName
				break
			}
		}

		if !found {
			t.Fatalf("Expected to find bucket: %#v, in %#v", integrationConfig.BucketID, resp.Buckets)
		}
	}
}

func TestFileManagement(t *testing.T) {
	c, ok := liveTestRetryClient(t, true)
	if !ok {
		return
	}

	t.Run("Uploading a file", func(t *testing.T) {
		buf := bytes.NewBufferString("Hello world")
		res, err := c.UploadFile(integrationConfig.BucketID, UploadFileOptions{
			FileName:      "test",
			ContentType:   ContentTypeText,
			ContentLength: int64(buf.Len()),
			Body:          Closer(buf),
		})

		if err != nil {
			t.Fatalf("Failed to upload file: %s", err)
		}

		if res.BucketID != integrationConfig.BucketID {
			t.Fatalf("Expected bucket id of uploaded file to match (%#v != %#v)", res.BucketID, integrationConfig.BucketID)
		}

		if res.FileName != "test" {
			t.Fatalf("Expected filename of uploaded file to match (%#v != %#v)", res.FileName, "test")
		}

		if res.Action != ActionUpload {
			t.Fatalf("Expected state of uploaded file to match (%#v != %#v)", res.Action, ActionUpload)
		}
	})

	t.Run("Listing files", func(t *testing.T) {
		listedFiles, err := c.ListFileNames(integrationConfig.BucketID, nil)
		if err != nil {
			t.Fatalf("Failed to list files: %s", err)
		}

		if len(listedFiles.Files) == 0 {
			t.Fatalf("Failed to list files: %#v", listedFiles.Files)
		}
	})
}
