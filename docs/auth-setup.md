# OBB Auth Setup — How to Create Users

No SQL migration needed. Supabase Auth uses its own built-in schema.

---

## Step 1 — Enable Email Auth in Supabase

1. Go to your Supabase dashboard → **Authentication** → **Providers**
2. Make sure **Email** is enabled
3. Under **Email** settings, you can disable "Confirm email" if you want passwords to work immediately without email verification (recommended for an internal tool)

---

## Step 2 — Create Users

1. Go to **Authentication** → **Users** → **Add user**
2. Enter the email and a strong password
3. Click **Create user**

Repeat for each team member (Ting, Sheena, etc.)

---

## Step 3 — Set the Role

Each user needs a `role` field in their **User Metadata**.

1. In the Users list, click on the user you just created
2. Under **User Metadata**, click **Edit**
3. Set the value to:

   For an admin (Hasan, Ting):
   ```json
   { "role": "admin" }
   ```

   For a viewer (Sheena, ops team):
   ```json
   { "role": "viewer" }
   ```

4. Click **Save**

---

## Role Permissions

| Role | Can do |
|------|--------|
| `admin` | Everything — approve, reject, edit, sync, export |
| `viewer` | View all pages, download CSV exports — no writes |

If a user has no `role` in their metadata, they default to `viewer`.

---

## Step 4 — Test It

1. Open the app URL (e.g. `https://obb-real-d4e16a8bb2ff.herokuapp.com`)
2. You should be redirected to `/login`
3. Log in with the credentials you just created
4. Verify the role badge appears in the bottom-left of the sidebar

---

## Changing a Password

In Supabase dashboard → **Authentication** → **Users** → click the user → **Reset password** → enter new password.

---

## Removing a User

In Supabase dashboard → **Authentication** → **Users** → click the user → **Delete user**.

Their session cookies stop working immediately on next request (JWT validation fails → redirect to `/login`).

---

## Token Expiry

- Access token: 1 hour (auto-refreshed by the app)
- Refresh token: 30 days (stored in HttpOnly cookie)
- Users stay logged in for 30 days without re-entering credentials

To force everyone out immediately: Supabase dashboard → **Authentication** → **Configuration** → rotate the JWT secret. All existing tokens become invalid instantly.
