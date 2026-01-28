package xauth

import (
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/shaj13/go-guardian/v2/auth"
	"github.com/shaj13/go-guardian/v2/auth/strategies/basic"
)

type (
	authWeb2py struct {
		id       string
		email    string
		password string
	}
)

const (
	XUserID       string = "id"
	XUserEmail    string = "email"
	XUserPassword string = "password"
)

const (
	queryAuthWeb2py = `SELECT auth_user.id, auth_user.email, auth_user.password
		FROM auth_user 
		WHERE auth_user.email = ?`
)

func NewBasicWeb2py(db *sql.DB, hmacKey string) auth.Strategy {
	authFunc := func(ctx context.Context, r *http.Request, userName, password string) (auth.Info, error) {
		u, err := authenticateWeb2py(ctx, db, userName, password, hmacKey)
		if err != nil {
			return nil, fmt.Errorf("invalid credentials")
		}
		return auth.NewUserInfo(userName, u.id, u.Groups(), u.extensions()), nil
	}
	return basic.New(authFunc)
}

func authenticateWeb2py(ctx context.Context, db *sql.DB, email, password, hmacKey string) (*authWeb2py, error) {
	var user authWeb2py

	err := db.
		QueryRowContext(ctx, queryAuthWeb2py, email).
		Scan(&user.id, &user.email, &user.password)
	if err != nil {
		return nil, fmt.Errorf("invalid credentials")
	}

	if !verifyWeb2pyPassword(password, user.password, hmacKey) {
		return nil, fmt.Errorf("invalid credentials")
	}

	return &user, nil
}

func (n *authWeb2py) extensions() auth.Extensions {
	ext := make(auth.Extensions)
	ext.Set(XUserID, n.id)
	ext.Set(XUserEmail, n.email)
	ext.Set(XUserPassword, n.password)
	return ext
}

func (n *authWeb2py) Groups() []string {
	return []string{}
}

func verifyWeb2pyPassword(password, storedHash, hmacKey string) bool {
	parts := strings.Split(storedHash, "$")
	if len(parts) != 3 {
		return false
	}

	if parts[0] != "sha512" {
		return false
	}

	salt := parts[1]
	expectedHash := parts[2]

	// Extraire la clé HMAC
	// Le format est "sha512:uuid", on prend la partie après ":"
	var keyPart string
	if strings.Contains(hmacKey, ":") {
		keyPart = strings.Split(hmacKey, ":")[1]
	} else {
		keyPart = hmacKey
	}

	// Web2py avec HMAC: HMAC-SHA512(password, key_part + salt)
	var computedHash string

	// HMAC avec clé = key_part + salt
	mac := hmac.New(sha512.New, []byte(keyPart+salt))
	mac.Write([]byte(password))
	computedHashBytes := mac.Sum(nil)
	computedHash = fmt.Sprintf("%x", computedHashBytes)

	match := hmac.Equal([]byte(computedHash), []byte(expectedHash))

	return match
}
