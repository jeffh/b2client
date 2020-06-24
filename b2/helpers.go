package b2

import (
	"fmt"
	"io"
	"os"
	"time"
)

func CredentialsFromEnv() Credentials { return CredentialsFromEnvPrefix("") }

func CredentialsFromEnvPrefix(prefix string) Credentials {
	getenv := func(keys ...string) string {
		for _, k := range keys {
			value := os.Getenv(k)
			if value != "" {
				return value
			}
		}
		return ""
	}

	return Credentials{
		KeyID:   getenv(prefix+"B2_KEY_ID", prefix+"B2_ACCOUNT_ID"),
		KeyName: getenv(prefix+"B2_KEY_NAME", prefix+"B2_ACCOUNT_NAME"),
		AppKey:  getenv(prefix+"B2_APP_KEY", prefix+"B2_ACCOUNT_KEY"),
	}
}

func logStrTime(t time.Time) string { return t.Format(time.RFC3339Nano) }

// Creates a range for b2 api [start, end] form (both sides are inclusive)
func InclusiveRange(startOffset, endOffset int) string {
	return fmt.Sprintf("%d-%d", startOffset, endOffset)
}

// Creates a range for b2 api [start, end) form (start is inclusive, end is exclusive)
func Range(startOffset, endOffset int) string {
	return fmt.Sprintf("%d-%d", startOffset, endOffset-1)
}

// Closer is a helper function to convert an io.Reader to an io.ReadCloser that has a no-op close method
func Closer(r io.Reader) io.ReadCloser { return &closable{r} }

type closable struct{ io.Reader }

func (c *closable) Close() error { return nil }
