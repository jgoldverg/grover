package backend

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/BurntSushi/toml"
	"github.com/google/uuid"
)

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

type SSHCredential struct {
	Name           string    `toml:"name"`
	Username       string    `toml:"username"`
	Host           string    `toml:"host"`
	Port           int       `toml:"port,omitempty"`
	PrivateKeyPath string    `toml:"private_key_path"`
	PublicKeyPath  string    `toml:"public_key_path,omitempty"`
	PublicKey      string    `toml:"public_key,omitempty"`
	PrivateKey     string    `toml:"private_key,omitempty"`
	UUID           uuid.UUID `toml:"uuid"`
	UseAgent       bool      `toml:"use_agent,omitempty"`
}

func (s *SSHCredential) GetUrl() string     { return fmt.Sprintf("%s:%s", s.Host, strconv.Itoa(s.Port)) }
func (s *SSHCredential) GetName() string    { return s.Name }
func (s *SSHCredential) GetType() string    { return "ssh" }
func (s *SSHCredential) GetUUID() uuid.UUID { return s.UUID }
func (s *SSHCredential) Validate() error {
	if s.Name == "" {
		return errors.New("name is required")
	}
	if s.Username == "" {
		return errors.New("username is required")
	}
	if s.Host == "" {
		return errors.New("host is required")
	}
	if s.Port == 0 {
		s.Port = 22
	}
	if s.PrivateKeyPath == "" && !s.UseAgent && s.PublicKey == "" {
		return errors.New("at least one authentication method required: private_key_path, use_agent, or public_key")
	}
	return nil
}

//We also need to implement a TLS base authentication method

type BasicAuthCredential struct {
	Name     string    `toml:"name"`
	Username string    `toml:"username"`
	Password string    `toml:"password"`
	URL      string    `toml:"url"`
	UUID     uuid.UUID `toml:"uuid"`
}

func (c *BasicAuthCredential) GetUrl() string {
	return c.URL
}

func (c *BasicAuthCredential) GetUserName() string {
	return c.Username
}
func (c *BasicAuthCredential) GetPassword() string {
	return c.Password
}
func (c *BasicAuthCredential) GetName() string    { return c.Name }
func (c *BasicAuthCredential) GetType() string    { return "basic" }
func (c *BasicAuthCredential) GetUUID() uuid.UUID { return c.UUID }
func (c *BasicAuthCredential) Validate() error {
	if c.Name == "" {
		return errors.New("name is required")
	}
	if c.URL == "" {
		return errors.New("url is required")
	}
	return nil
}

// Add JWTCredential, S3Credential similarly...

// Wrapper struct for TOML unmarshalling and marshalling

type CredentialEntry struct {
	Type  string               `toml:"type"`
	SSH   *SSHCredential       `toml:"ssh,omitempty"`
	Basic *BasicAuthCredential `toml:"basic,omitempty"`
	// JWT, S3, etc. can be added here
}

func (ce CredentialEntry) ToCredential() (Credential, error) {
	switch ce.Type {
	case "ssh":
		if ce.SSH == nil {
			return nil, errors.New("ssh field missing")
		}
		return ce.SSH, nil
	case "basic":
		if ce.Basic == nil {
			return nil, errors.New("basic field missing")
		}
		return ce.Basic, nil
	default:
		return nil, errors.New("unknown credential type: %s" + ce.Type)
	}
}

func FromCredential(cred Credential) (CredentialEntry, error) {
	switch c := cred.(type) {
	case *SSHCredential:
		return CredentialEntry{Type: "ssh", SSH: c}, nil
	case *BasicAuthCredential:
		return CredentialEntry{Type: "basic", Basic: c}, nil
	// case *JWTCredential:
	// 	return CredentialEntry{Type: "jwt", JWT: c}, nil
	// case *S3Credential:
	// 	return CredentialEntry{Type: "s3", S3: c}, nil
	default:
		return CredentialEntry{}, errors.New("unsupported credential type")
	}
}

// TOML storage implementation

type TomlCredentialStorage struct {
	filePath    string
	Credentials map[string]CredentialEntry `toml:"credentials"`
}

func NewTomlCredentialStorage(filePath string) (CredentialStorage, error) {
	storage := &TomlCredentialStorage{
		filePath:    filePath,
		Credentials: make(map[string]CredentialEntry),
	}

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		err = os.MkdirAll(filepath.Dir(filePath), 0755)
		if err != nil {
			return nil, err
		}
		f, err := os.Create(filePath)
		if err != nil {
			return nil, err
		}
		f.Close()
	}

	if err := storage.loadFromFile(); err != nil {
		return nil, err
	}

	return storage, nil
}

func (s *TomlCredentialStorage) loadFromFile() error {
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no file yet, treat as empty
		}
		return err
	}

	return toml.Unmarshal(data, s)
}

func (s *TomlCredentialStorage) saveToFile() error {
	var buf bytes.Buffer
	encoder := toml.NewEncoder(&buf)
	if err := encoder.Encode(s); err != nil {
		return err
	}
	err := os.WriteFile(s.filePath, buf.Bytes(), 0644)
	if err != nil {
		return errors.New("failed to save credential storage: " + err.Error())
	}
	return nil
}

func (s *TomlCredentialStorage) GetCredentialByUUID(id uuid.UUID) (Credential, error) {
	entry, ok := s.Credentials[id.String()]
	if !ok {
		return nil, errors.New("credential not found")
	}
	return entry.ToCredential()
}

func (s *TomlCredentialStorage) GetCredentialByName(name string) (Credential, error) {
	for _, entry := range s.Credentials {
		cred, err := entry.ToCredential()
		if err != nil {
			continue
		}
		if cred.GetName() == name {
			return cred, nil
		}
	}
	return nil, errors.New("credential not found")
}

func (s *TomlCredentialStorage) AddCredential(cred Credential) error {
	if cred.GetUUID() == uuid.Nil {
		return errors.New("credential must have a UUID")
	}
	if err := cred.Validate(); err != nil {
		return err
	}
	entry, err := FromCredential(cred)
	if err != nil {
		return err
	}

	s.Credentials[cred.GetUUID().String()] = entry
	return s.saveToFile()
}

func (s *TomlCredentialStorage) DeleteCredential(id uuid.UUID) error {
	if _, exists := s.Credentials[id.String()]; !exists {
		return errors.New("credential not found")
	}
	delete(s.Credentials, id.String())
	return s.saveToFile()
}

func (s *TomlCredentialStorage) DeleteCredentialByName(name string) error {
	for id, entry := range s.Credentials {
		cred, err := entry.ToCredential()
		if err != nil {
			continue
		}
		if cred.GetName() == name {
			delete(s.Credentials, id)
			return s.saveToFile()
		}
	}
	return errors.New("credential not found")
}

func (s *TomlCredentialStorage) ListCredentials() ([]Credential, error) {
	var creds []Credential
	for _, entry := range s.Credentials {
		cred, err := entry.ToCredential()
		if err == nil {
			creds = append(creds, cred)
		}
	}
	return creds, nil
}

func (s *TomlCredentialStorage) ListCredentialsByType(typ string) ([]Credential, error) {
	if typ == "" {
		return s.ListCredentials()
	}
	var creds []Credential
	for _, entry := range s.Credentials {
		if entry.Type == typ {
			cred, err := entry.ToCredential()
			if err == nil {
				creds = append(creds, cred)
			}
		}
	}
	return creds, nil
}
