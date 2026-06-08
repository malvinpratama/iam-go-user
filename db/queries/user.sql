-- name: CreateProfile :one
INSERT INTO profiles (user_id, display_name)
VALUES ($1, $2)
RETURNING user_id, display_name, bio, avatar_url, phone, created_at, updated_at;

-- name: GetProfile :one
SELECT user_id, display_name, bio, avatar_url, phone, created_at, updated_at
FROM profiles
WHERE user_id = $1;

-- name: UpdateProfile :one
UPDATE profiles
SET display_name = COALESCE(sqlc.narg('display_name'), display_name),
    bio          = COALESCE(sqlc.narg('bio'), bio),
    avatar_url   = COALESCE(sqlc.narg('avatar_url'), avatar_url),
    phone        = COALESCE(sqlc.narg('phone'), phone),
    updated_at   = now()
WHERE user_id = sqlc.arg('user_id')
RETURNING user_id, display_name, bio, avatar_url, phone, created_at, updated_at;

-- name: DeleteProfile :exec
DELETE FROM profiles WHERE user_id = $1;

-- name: ListProfiles :many
SELECT user_id, display_name, bio, avatar_url, phone, created_at, updated_at
FROM profiles
WHERE ($1::text = '' OR display_name ILIKE '%' || $1 || '%')
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: CountProfiles :one
SELECT count(*)
FROM profiles
WHERE ($1::text = '' OR display_name ILIKE '%' || $1 || '%');

-- name: UpsertProfile :exec
-- Idempotent profile creation for the event consumer (at-least-once delivery).
INSERT INTO profiles (user_id, display_name)
VALUES ($1, $2)
ON CONFLICT (user_id) DO NOTHING;
