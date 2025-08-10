package fs

import "github.com/google/uuid"

type Credential interface {
	GetName() string
	GetType() string
	GetUrl() string
	Validate() error
	GetUUID() uuid.UUID
}

type CredentialStorage interface {
	GetCredentialByUUID(uuid.UUID) (Credential, error)
	GetCredentialByName(name string) (Credential, error)
	AddCredential(Credential) error
	DeleteCredential(uuid.UUID) error
	DeleteCredentialByName(string) error
	ListCredentials() ([]Credential, error)
	ListCredentialsByType(string) ([]Credential, error)
}
