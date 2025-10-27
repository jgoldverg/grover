package gclient

import (
	"context"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/util"
	"google.golang.org/grpc"
)

type CredentialService struct {
	api       pb.CredentialServiceClient
	storage   backend.CredentialStorage
	policy    util.RoutePolicy
	hasRemote bool
}

func NewCredentialService(cfg *internal.AppConfig, conn grpc.ClientConnInterface, policy util.RoutePolicy) (*CredentialService, error) {
	store, err := backend.NewTomlCredentialStorage(cfg.CredentialsFile)
	if err != nil {
		return nil, err
	}
	cs := &CredentialService{
		hasRemote: false,
		storage:   store,
		policy:    policy,
		api:       nil,
	}

	if conn != nil {
		cs.api = pb.NewCredentialServiceClient(conn)
		cs.hasRemote = true
	}
	return cs, nil
}

func (c *CredentialService) AddCredential(ctx context.Context, cred backend.Credential) error {
	if util.ShouldUseRemote(c.policy, c.hasRemote) {
		credPb := &pb.Credential{
			CredentialUuid: "",
			CredentialName: "",
			Type:           0,
			Details:        nil,
		}
		switch v := cred.(type) {
		case *backend.BasicAuthCredential:
			credPb.Details = &pb.Credential_Basic{Basic: &pb.BasicDetails{
				Username: v.GetUserName(),
				Password: v.GetPassword(),
				Url:      v.GetUrl(),
			}}
		case *backend.SSHCredential:
			credPb.Details = &pb.Credential_Ssh{Ssh: &pb.SshDetails{
				Username:   v.Username,
				Host:       v.Host,
				Port:       int32(v.Port),
				PublicKey:  v.PublicKey,
				PrivateKey: v.PrivateKey,
				UseAgent:   v.UseAgent,
			}}
		}

		req := pb.CreateCredentialRequest{Credential: credPb}
		_, err := c.api.Create(ctx, &req)
		if err != nil {
			return err
		}
	} else {
		err := c.storage.AddCredential(cred)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *CredentialService) ListCredentials(ctx context.Context, credType string) ([]backend.Credential, error) {
	if util.ShouldUseRemote(c.policy, c.hasRemote) {
		req := pb.ListCredentialsRequest{}
		switch credType {
		case "basic":
			req.Type = pb.CredentialType_BASIC_CREDENTIAL_TYPE
		case "ssh":
			req.Type = pb.CredentialType_SSH_CREDENTIAL_TYPE
		default:
			req.Type = pb.CredentialType_CREDENTIAL_TYPE_UNSPECIFIED
		}
		resp, err := c.api.List(ctx, &req)
		if err != nil {
			return nil, err
		}
		return util.PbCredentialToBackendCredential(resp.GetCredentials()), nil
	} else {
		return c.storage.ListCredentialsByType(credType)
	}
}

func (c *CredentialService) DeleteCredential(ctx context.Context, credUuid uuid.UUID, credName string) error {
	if util.ShouldUseRemote(c.policy, c.hasRemote) {
		req := pb.DeleteCredentialRequest{}
		if credUuid != uuid.Nil {
			credRef := pb.CredentialRef{
				Ref: &pb.CredentialRef_CredentialUuid{CredentialUuid: credUuid.String()},
			}
			req.Ref = &credRef
		}
		if credName != "" {
			credRef := pb.CredentialRef{
				Ref: &pb.CredentialRef_CredentialName{CredentialName: credName},
			}
			req.Ref = &credRef
		}
		_, err := c.api.Delete(ctx, &req)
		return err
	} else {
		if credUuid != uuid.Nil {
			return c.storage.DeleteCredential(credUuid)
		} else {
			return c.storage.DeleteCredentialByName(credName)
		}
	}
}
