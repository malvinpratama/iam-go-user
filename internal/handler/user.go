// Package handler implements the UserService gRPC server.
package handler

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	userv1 "github.com/malvinpratama/iam-go-contracts/gen/user/v1"
	"github.com/malvinpratama/iam-go-user/internal/db"
)

// UserHandler implements userv1.UserServiceServer.
type UserHandler struct {
	userv1.UnimplementedUserServiceServer
	q *db.Queries
}

// New builds a UserHandler.
func New(pool *pgxpool.Pool) *UserHandler {
	return &UserHandler{q: db.New(pool)}
}

func (h *UserHandler) CreateProfile(ctx context.Context, req *userv1.CreateProfileRequest) (*userv1.Profile, error) {
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user id")
	}
	p, err := h.q.CreateProfile(ctx, db.CreateProfileParams{UserID: userID, DisplayName: req.GetDisplayName()})
	if err != nil {
		return nil, status.Error(codes.AlreadyExists, "profile already exists")
	}
	return toProto(p), nil
}

func (h *UserHandler) GetProfile(ctx context.Context, req *userv1.GetProfileRequest) (*userv1.Profile, error) {
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user id")
	}
	p, err := h.q.GetProfile(ctx, userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "profile not found")
		}
		return nil, status.Error(codes.Internal, "failed to get profile")
	}
	return toProto(p), nil
}

func (h *UserHandler) UpdateProfile(ctx context.Context, req *userv1.UpdateProfileRequest) (*userv1.Profile, error) {
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user id")
	}
	p, err := h.q.UpdateProfile(ctx, db.UpdateProfileParams{
		UserID:      userID,
		DisplayName: req.DisplayName,
		Bio:         req.Bio,
		AvatarUrl:   req.AvatarUrl,
		Phone:       req.Phone,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "profile not found")
		}
		return nil, status.Error(codes.Internal, "failed to update profile")
	}
	return toProto(p), nil
}

func (h *UserHandler) DeleteProfile(ctx context.Context, req *userv1.DeleteProfileRequest) (*userv1.DeleteProfileResponse, error) {
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user id")
	}
	// Soft by default (stamps deleted_at); hard removes the row entirely.
	del := h.q.DeleteProfile
	if req.GetHard() {
		del = h.q.HardDeleteProfile
	}
	if err := del(ctx, userID); err != nil {
		return nil, status.Error(codes.Internal, "failed to delete profile")
	}
	return &userv1.DeleteProfileResponse{Success: true}, nil
}

func (h *UserHandler) RestoreProfile(ctx context.Context, req *userv1.RestoreProfileRequest) (*userv1.DeleteProfileResponse, error) {
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user id")
	}
	if err := h.q.RestoreProfile(ctx, userID); err != nil {
		return nil, status.Error(codes.Internal, "failed to restore profile")
	}
	return &userv1.DeleteProfileResponse{Success: true}, nil
}

func (h *UserHandler) ListProfiles(ctx context.Context, req *userv1.ListProfilesRequest) (*userv1.ListProfilesResponse, error) {
	page := req.GetPage()
	if page < 1 {
		page = 1
	}
	size := req.GetPageSize()
	if size < 1 {
		size = 20
	}
	if size > 100 {
		size = 100
	}
	offset := (page - 1) * size

	// deleted_only flips the view to soft-deleted profiles (for the restore UI).
	var rows []db.Profile
	var total int64
	var err error
	if req.GetDeletedOnly() {
		rows, err = h.q.ListDeletedProfiles(ctx, db.ListDeletedProfilesParams{Column1: req.GetQuery(), Limit: size, Offset: offset})
		if err == nil {
			total, err = h.q.CountDeletedProfiles(ctx, req.GetQuery())
		}
	} else {
		rows, err = h.q.ListProfiles(ctx, db.ListProfilesParams{Column1: req.GetQuery(), Limit: size, Offset: offset})
		if err == nil {
			total, err = h.q.CountProfiles(ctx, req.GetQuery())
		}
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to list profiles")
	}
	out := make([]*userv1.Profile, 0, len(rows))
	for _, p := range rows {
		out = append(out, toProto(p))
	}
	return &userv1.ListProfilesResponse{Profiles: out, Total: int32(total), Page: page, PageSize: size}, nil
}

func toProto(p db.Profile) *userv1.Profile {
	return &userv1.Profile{
		UserId:      p.UserID.String(),
		DisplayName: p.DisplayName,
		Bio:         p.Bio,
		AvatarUrl:   p.AvatarUrl,
		Phone:       p.Phone,
		CreatedAt:   tsString(p.CreatedAt),
		UpdatedAt:   tsString(p.UpdatedAt),
	}
}

func tsString(t pgtype.Timestamptz) string {
	if !t.Valid {
		return ""
	}
	return t.Time.UTC().Format("2006-01-02T15:04:05Z07:00")
}
