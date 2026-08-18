package controlplane

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	authCookieName = "dualroute_gateway_session"
	sessionTTL     = 8 * time.Hour
	pbkdf2Rounds   = 120000
)

type adminCredential struct {
	Username           string `json:"username"`
	Salt               []byte `json:"salt"`
	PasswordHash       []byte `json:"password_hash"`
	MustChangePassword bool   `json:"must_change_password"`
}

type session struct {
	ExpiresAt time.Time
}

func (s *Server) loadInstanceToken() {
	if strings.TrimSpace(s.cfg.InstanceToken) != "" {
		s.persistInstanceToken(s.cfg.InstanceToken)
		return
	}
	if s.cfg.DataDir != "" {
		if data, err := os.ReadFile(filepath.Join(s.cfg.DataDir, "instance-token")); err == nil && strings.TrimSpace(string(data)) != "" {
			s.cfg.InstanceToken = strings.TrimSpace(string(data))
			return
		}
	}
	s.cfg.InstanceToken = randomSecret(32)
	s.persistInstanceToken(s.cfg.InstanceToken)
}

func (s *Server) persistInstanceToken(token string) {
	if s.cfg.DataDir == "" || token == "" {
		return
	}
	_ = os.MkdirAll(s.cfg.DataDir, 0o700)
	_ = os.WriteFile(filepath.Join(s.cfg.DataDir, "instance-token"), []byte(token+"\n"), 0o600)
}

func (s *Server) loadAuth() {
	if s.cfg.DataDir != "" {
		data, err := os.ReadFile(filepath.Join(s.cfg.DataDir, "admin.json"))
		if err == nil {
			var stored adminCredential
			if json.Unmarshal(data, &stored) == nil && stored.Username != "" && len(stored.Salt) >= 16 && len(stored.PasswordHash) == sha256.Size {
				s.auth = stored
				return
			}
		}
	}
	s.auth = newCredential("admin", "admin", true)
	s.persistAuthLocked()
}

func newCredential(username, password string, mustChange bool) adminCredential {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		panic("cryptographic random source unavailable")
	}
	return adminCredential{Username: username, Salt: salt, PasswordHash: pbkdf2SHA256([]byte(password), salt, pbkdf2Rounds, sha256.Size), MustChangePassword: mustChange}
}

func (s *Server) persistAuthLocked() {
	if s.cfg.DataDir == "" {
		return
	}
	_ = os.MkdirAll(s.cfg.DataDir, 0o700)
	data, _ := json.MarshalIndent(s.auth, "", "  ")
	_ = os.WriteFile(filepath.Join(s.cfg.DataDir, "admin.json"), data, 0o600)
}

func (s *Server) authStatus(w http.ResponseWriter, r *http.Request) {
	_, authenticated, mustChange := s.authenticated(r)
	writeJSON(w, http.StatusOK, map[string]any{"authenticated": authenticated, "must_change_password": authenticated && mustChange})
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 8<<10)).Decode(&body) != nil {
		http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
		return
	}
	s.authMu.Lock()
	valid := strings.TrimSpace(body.Username) == s.auth.Username && verifyPassword(s.auth, body.Password)
	mustChange := s.auth.MustChangePassword
	s.authMu.Unlock()
	if !valid {
		http.Error(w, `{"error":"invalid_credentials"}`, http.StatusUnauthorized)
		return
	}
	s.createSession(w, r)
	writeJSON(w, http.StatusOK, map[string]any{"must_change_password": mustChange})
}

func (s *Server) changePassword(w http.ResponseWriter, r *http.Request) {
	_, authenticated, _ := s.authenticated(r)
	if !authenticated {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	var body struct {
		Password string `json:"password"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 8<<10)).Decode(&body) != nil {
		http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
		return
	}
	if len(body.Password) < 8 || len(body.Password) > 256 {
		http.Error(w, `{"error":"invalid_password"}`, http.StatusBadRequest)
		return
	}
	s.authMu.Lock()
	s.auth = newCredential(s.auth.Username, body.Password, false)
	s.sessions = make(map[string]session)
	s.persistAuthLocked()
	s.authMu.Unlock()
	s.createSession(w, r)
	writeJSON(w, http.StatusOK, map[string]bool{"changed": true})
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(authCookieName); err == nil {
		s.authMu.Lock()
		delete(s.sessions, cookie.Value)
		s.authMu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: authCookieName, Value: "", Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: r.TLS != nil})
	writeJSON(w, http.StatusOK, map[string]bool{"logged_out": true})
}

func (s *Server) authenticated(r *http.Request) (string, bool, bool) {
	cookie, err := r.Cookie(authCookieName)
	if err != nil || cookie.Value == "" {
		return "", false, false
	}
	now := time.Now()
	s.authMu.Lock()
	defer s.authMu.Unlock()
	for id, current := range s.sessions {
		if now.After(current.ExpiresAt) {
			delete(s.sessions, id)
		}
	}
	current, ok := s.sessions[cookie.Value]
	if !ok || now.After(current.ExpiresAt) {
		return "", false, false
	}
	return s.auth.Username, true, s.auth.MustChangePassword
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	id := randomSecret(32)
	expires := time.Now().Add(sessionTTL)
	s.authMu.Lock()
	s.sessions[id] = session{ExpiresAt: expires}
	s.authMu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: authCookieName, Value: id, Path: "/", Expires: expires, MaxAge: int(sessionTTL.Seconds()), HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: r.TLS != nil})
}

func verifyPassword(credential adminCredential, password string) bool {
	hash := pbkdf2SHA256([]byte(password), credential.Salt, pbkdf2Rounds, len(credential.PasswordHash))
	return len(hash) == len(credential.PasswordHash) && subtle.ConstantTimeCompare(hash, credential.PasswordHash) == 1
}

func randomSecret(bytes int) string {
	buffer := make([]byte, bytes)
	if _, err := rand.Read(buffer); err != nil {
		panic("cryptographic random source unavailable")
	}
	return hex.EncodeToString(buffer)
}

// pbkdf2SHA256 is a small local implementation that keeps the control plane
// dependency-free while storing passwords with a deliberate work factor.
func pbkdf2SHA256(password, salt []byte, rounds, length int) []byte {
	result := make([]byte, 0, length)
	block := make([]byte, len(salt)+4)
	copy(block, salt)
	for blockNumber := uint32(1); len(result) < length; blockNumber++ {
		block[len(salt)] = byte(blockNumber >> 24)
		block[len(salt)+1] = byte(blockNumber >> 16)
		block[len(salt)+2] = byte(blockNumber >> 8)
		block[len(salt)+3] = byte(blockNumber)
		mac := hmac.New(sha256.New, password)
		_, _ = mac.Write(block)
		u := mac.Sum(nil)
		t := append([]byte(nil), u...)
		for round := 1; round < rounds; round++ {
			mac = hmac.New(sha256.New, password)
			_, _ = mac.Write(u)
			u = mac.Sum(nil)
			for i := range t {
				t[i] ^= u[i]
			}
		}
		result = append(result, t...)
	}
	return result[:length]
}
