package b2

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func runIntegrationTests() bool { return true }

var integrationConfig = struct {
	BucketID   string
	BucketName string
}{}

func init() {
	integrationConfig.BucketID = os.Getenv("TEST_B2_BUCKET_ID")
	integrationConfig.BucketName = os.Getenv("TEST_B2_BUCKET_NAME")

	if integrationConfig.BucketID == "" {
		panic(fmt.Errorf("TEST_B2_BUCKET_ID is missing"))
	}

	if integrationConfig.BucketName == "" {
		panic(fmt.Errorf("TEST_B2_BUCKET_NAME is missing"))
	}
}

func assertAuth(t *testing.T, resp AuthorizeAccountResponse) (AuthorizeAccountResponse, bool) {
	if !(resp.AbsoluteMinimumPartSize > 0) {
		t.Fatalf("Expected AbsoluteMinimumPartSize to be set, got: 0 -- %#v", resp)
		return resp, false
	}

	if !(resp.RecommendedPartSize > 0) {
		t.Fatalf("Expected RecommendedPartSize to be set, got: 0 -- %#v", resp)
		return resp, false
	}

	if !(resp.AccountID != "") {
		t.Fatalf("Expected AccountId to be set -- %#v", resp)
		return resp, false
	}

	if !(resp.APIURL != "") {
		t.Fatalf("Expected APIURL to be set -- %#v", resp)
		return resp, false
	}

	if !(resp.AuthorizationToken != "") {
		t.Fatalf("Expected AuthorizationToken to be set -- %#v", resp)
		return resp, false
	}

	if !(resp.DownloadURL != "") {
		t.Fatalf("Expected AuthorizationToken to be set -- %#v", resp)
		return resp, false
	}

	if !(len(resp.Allowed.Capabilities) > 0) {
		t.Fatalf("Expected Capabilities to be not empty, got %#v", resp.Allowed.Capabilities)
		return resp, false
	}
	return resp, true
}

func mustAuth(t *testing.T, clt *Client) (AuthorizeAccountResponse, bool) {
	creds := CredentialsFromEnvPrefix("TEST_")

	if creds.KeyID == "" {
		t.Fatalf("Expected credentials, got: %#v", creds)
		return AuthorizeAccountResponse{}, false
	}

	resp, err := clt.Authorize(creds.KeyID, creds.AppKey)
	if err != nil {
		t.Fatalf("Expected authorization to not error: got -- %s", err)
		return resp, false
	}

	return assertAuth(t, resp)
}

var (
	cachedClient      *Client
	cachedRetryClient *RetryClient
)

func liveTestRetryClient(t *testing.T, allowReuse bool) (clt *RetryClient, ok bool) {
	if !runIntegrationTests() {
		return nil, false
	}
	if allowReuse && cachedRetryClient != nil {
		return cachedRetryClient, true
	}
	creds := CredentialsFromEnvPrefix("TEST_")
	clt = &RetryClient{
		KeyID:  creds.KeyID,
		AppKey: creds.AppKey,
		C:      Client{L: log.New(os.Stdout, "[LiveTestRetryClient] ", log.LstdFlags)},
	}
	cachedRetryClient = clt
	return clt, true
}
