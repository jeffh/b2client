package b2

import (
	"fmt"
	"runtime"
)

const ClientVersion = "0.1.0"

func DefaultUserAgent() string {
	return fmt.Sprintf("net.jeffhui.b2client/%s+%s", ClientVersion, runtime.Version())
}

const (
	// see https://www.backblaze.com/b2/docs/content-types.html
	ContentTypeHide = "application/x-bz-hide-marker"
	ContentTypeAuto = "b2/x-auto"
	ContentTypeText = "text/plain"
)

const Sha1AtEnd = "hex_digits_at_end"

type Credentials struct {
	KeyID   string // also known as appId
	KeyName string
	AppKey  string
}

func (c *Credentials) AppId() string { return c.KeyID }

type FileInfo map[string]interface{}
type BucketInfo map[string]interface{}

type BucketType string

const (
	BucketTypePublic   BucketType = "allPublic"
	BucketTypePrivate             = "allPrivate"
	BucketTypeSnapshot            = "snapshot"
	BucketTypeAll                 = "all" // special type only for ListBuckets
)

const (
	CapabilityListKeys      = "listKeys"
	CapabilityWriteKeys     = "writeKeys"
	CapabilityDeleteKeys    = "deleteKeys"
	CapabilityListBuckets   = "listBuckets"
	CapabilityWriteBuckets  = "writeBuckets"
	CapabilityDeleteBuckets = "deleteBuckets"
	CapabilityListFiles     = "listFiles"
	CapabilityReadFiles     = "readFiles"
	CapabilityShareFiles    = "shareFiles"
	CapabilityWriteFiles    = "writeFiles"
	CapabilityDeleteFiles   = "deleteFiles"
)

type CorsRule struct {
	CorsRuleName   string   `json:"corsRuleName"`   // required
	AllowedOrigins []string `json:"allowedOrigins"` // required

	// Allowed operations:
	//  - b2_download_file_by_name
	//  - b2_download_file_by_id
	//  - b2_upload_file
	//  - b2_upload_part
	AllowedOperations []string `json:"allowedOperations"` // required
	AllowedHeaders    []string `json:"allowedHeaders,omitempty"`
	ExposeHeaders     []string `json:"exposeHeaders,omitempty"`
	MaxAgeSeconds     int      `json:"maxAgeSeconds"` // required
}

type LifecycleRule struct {
	FileNamePrefix            string `json:"fileNamePrefix"`
	DaysFromHidingToDeleting  *int   `json:"daysFromHidingToDeleting"`
	DaysFromUploadingToHiding *int   `json:"daysFromUploadingToHiding"`
}

type Action string

const (
	ActionStart  = "start"  // a large file has been started, but not finished or canceled
	ActionUpload = "upload" // a file was uploaded to B2
	ActionHide   = "hide"   //file version marking the file as hidden, so it doesn't show up in list_file_names
	ActionFolder = "folder" // virtual folder when listing files
)

type requestByFileID struct {
	FileID string `json:"fileId"`
}

type requestByFileName struct {
	BucketID string `json:"bucketId"`
	FileName string `json:"fileName"`
}

type Bucket struct {
	AccountID      string          `json:"accountId"`
	BucketID       string          `json:"bucketId"`
	BucketName     string          `json:"bucketName"`
	BucketType     BucketType      `json:"bucketType"`
	BucketInfo     BucketInfo      `json:"bucketInfo,omitempty"`
	CorsRules      []CorsRule      `json:"corsRules,omitempty"`
	LifecycleRules []LifecycleRule `json:"lifecycleRules,omitempty"`
	Revision       int             `json:"revision"`
}

type File struct {
	AccountID       string   `json:"accountId"`
	BucketID        string   `json:"bucketId"`
	FileID          string   `json:"fileId"`
	FileName        string   `json:"fileName"`
	Action          Action   `json:"action"`
	ContentLength   int64    `json:"contentLength"`
	ContentSha1     string   `json:"contentSha1"`
	ContentMd5      string   `json:"contentMd5,omitempty"`
	ContentType     string   `json:"contentType"`
	FileInfo        FileInfo `json:"fileInfo"`
	UploadTimestamp int64    `json:"uploadTimestamp"`
}

type FilePart struct {
	FileID          string `json:"fileId"`
	PartNumber      int    `json:"partNumber"`
	ContentLength   string `json:"contentLength"`
	ContentSha1     string `json:"contentSha1"`
	ContentMd5      string `json:"contentMd5,omitempty"`
	UploadTimestamp int64  `json:"uploadTimestamp"`
}

type Key struct {
	KeyName             string   `json:"keyName"`
	ApplicationKeyID    string   `json:"applicationKeyId"`
	ApplicationKey      string   `json:"applicationKey"`
	Capabilities        []string `json:"capabilities"`
	AccountID           string   `json:"accountId"`
	ExpirationTimestamp *int64   `json:"expirationTimestamp,omitempty"`
	BucketID            string   `json:"bucketId"`
	NamePrefix          string   `json:"namePrefix"`
}
