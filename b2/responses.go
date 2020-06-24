package b2

type AuthorizeAccountResponse struct {
	AbsoluteMinimumPartSize int                           `json:"absoluteMinimumPartSize"`
	RecommendedPartSize     int                           `json:"recommendedPartSize"`
	AccountID               string                        `json:"accountId"`
	Allowed                 AuthorizeAcccountCapabilities `json:"allowed"`
	APIURL                  string                        `json:"apiUrl"`
	AuthorizationToken      string                        `json:"authorizationToken"`
	DownloadURL             string                        `json:"downloadURL"`
}

type AuthorizeAcccountCapabilities struct {
	BucketID     string   `json:"bucketId"`
	BucketName   string   `json:"bucketName"`
	Capabilities []string `json:"capabilities"`
	NamePrefix   *string  `json:"namePrefix"`
}

type CancelLargeFileResponse struct {
	AccountID string `json:"accountId"`
	BucketID  string `json:"bucketId"`
	FileId    string `json:"fileId"`
	FileName  string `json:"fileName"`
}

type FileResponse File

type CopyFileResponse FileResponse

type CopyPartResponse FilePart

type BucketResponse Bucket

type KeyResponse Key

type DeleteFileResponse struct {
	FileID   string `json:"fileId"`
	FileName string `json:"fileName"`
}

type FinishLargeFileResponse FileResponse

type GetDownloadAuthorizationResponse struct {
	BucketID           string `json:"bucketId"`
	FileNamePrefix     string `json:"fileNamePrefix"`
	AuthorizationToken string `json:"authorizationToken"`
}

type GetFileInfoResponse FileResponse

type UploadURLResponse struct {
	FileID             string `json:"fileId"`
	UploadURL          string `json:"uploadUrl"`
	AuthorizationToken string `json:"authorizationToken"`
}

type GetUploadPartURLResponse UploadURLResponse
type GetUploadURLResponse UploadURLResponse

type HideFileResponse FileResponse

type ListBucketsResponse struct {
	Buckets []Bucket `json:"buckets"`
}

type ListFileNamesResponse struct {
	Files        []File `json:"files"`
	NextFileName string `json:"nextFileName"`
}

type ListFileVersionsResponse struct {
	Files        []File `json:"files"`
	NextFileName string `json:"nextFileName"`
	NextFileID   string `json:"nextFileId"`
}

type ListKeysResponse struct {
	Keys         []Key  `json:"keys"`
	NextAppKeyId string `json:"nextApplicationKeyId"`
}

type ListPartsResponse struct {
	Parts          []FilePart `json:"parts"`
	NextPartNumber int        `json:"nextPartNumber"`
}

type ListUnfinishedLargeFilesResponse struct {
	Files      []File `json:"files"`
	NextFileID string `json:"nextFileId"`
}

type StartLargeFileResponse FileResponse

type UpdateBucketResponse BucketResponse

type UploadFileResponse FileResponse

type UploadPartResponse FilePart
