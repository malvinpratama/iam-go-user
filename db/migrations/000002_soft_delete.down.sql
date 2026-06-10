DROP INDEX IF EXISTS idx_profiles_deleted_at;
ALTER TABLE profiles DROP COLUMN IF EXISTS deleted_at;
