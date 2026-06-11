-- name: CreateProfile :one
INSERT INTO profiles (user_id, display_name)
VALUES ($1, $2)
RETURNING user_id, display_name, bio, avatar_url, phone, created_at, updated_at, deleted_at;

-- name: GetProfile :one
SELECT user_id, display_name, bio, avatar_url, phone, created_at, updated_at, deleted_at
FROM profiles
WHERE user_id = $1 AND deleted_at IS NULL;

-- name: UpdateProfile :one
UPDATE profiles
SET display_name = COALESCE(sqlc.narg('display_name'), display_name),
    bio          = COALESCE(sqlc.narg('bio'), bio),
    avatar_url   = COALESCE(sqlc.narg('avatar_url'), avatar_url),
    phone        = COALESCE(sqlc.narg('phone'), phone),
    updated_at   = now()
WHERE user_id = sqlc.arg('user_id') AND deleted_at IS NULL
RETURNING user_id, display_name, bio, avatar_url, phone, created_at, updated_at, deleted_at;

-- name: DeleteProfile :exec
UPDATE profiles SET deleted_at = now(), updated_at = now()
WHERE user_id = $1 AND deleted_at IS NULL;

-- name: RestoreProfile :exec
UPDATE profiles SET deleted_at = NULL, updated_at = now() WHERE user_id = $1;

-- name: HardDeleteProfile :exec
DELETE FROM profiles WHERE user_id = $1;

-- name: ListProfiles :many
SELECT user_id, display_name, bio, avatar_url, phone, created_at, updated_at, deleted_at
FROM profiles
WHERE deleted_at IS NULL AND ($1::text = '' OR display_name ILIKE '%' || $1 || '%')
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: CountProfiles :one
SELECT count(*)
FROM profiles
WHERE deleted_at IS NULL AND ($1::text = '' OR display_name ILIKE '%' || $1 || '%');

-- name: ListDeletedProfiles :many
SELECT user_id, display_name, bio, avatar_url, phone, created_at, updated_at, deleted_at
FROM profiles
WHERE deleted_at IS NOT NULL AND ($1::text = '' OR display_name ILIKE '%' || $1 || '%')
ORDER BY deleted_at DESC
LIMIT $2 OFFSET $3;

-- name: CountDeletedProfiles :one
SELECT count(*)
FROM profiles
WHERE deleted_at IS NOT NULL AND ($1::text = '' OR display_name ILIKE '%' || $1 || '%');

-- name: UpsertProfile :exec
-- Idempotent profile creation for the event consumer (at-least-once delivery).
-- On re-registration of a previously soft-deleted user, clear deleted_at.
INSERT INTO profiles (user_id, display_name)
VALUES ($1, $2)
ON CONFLICT (user_id) DO UPDATE SET deleted_at = NULL;
