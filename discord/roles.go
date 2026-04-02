package discord

import (
	"context"
	"fmt"

	"discodb/types"

	"github.com/bwmarrin/discordgo"
)

// RoleCreateParams contains parameters for creating a role.
// In discodb, roles are used for Free Space Map (FSM) pages.
type RoleCreateParams struct {
	Name        string // Role name - used as FSM page address
	Color       int    // 24-bit color - used for metadata
	Permissions int64  // 64-bit permissions - used for trit storage (40 slots)
	Position    int    // Role position
	Mentionable bool
}

// RoleEditParams contains parameters for editing a role.
type RoleEditParams struct {
	Name        *string
	Color       *int
	Permissions *int64
	Position    *int
	Mentionable *bool
}

// ListRoles retrieves all roles in a guild.
func (c *Client) ListRoles(ctx context.Context, guildID types.GuildID) ([]*Role, error) {
	const op = "ListRoles"

	var results []*Role
	err := c.withRetry(ctx, op, func() error {
		roles, err := c.session.GuildRoles(guildIDToString(guildID), c.requestOption(ctx)...)
		if err != nil {
			return wrapError(op, err)
		}

		results = make([]*Role, 0, len(roles))
		for _, r := range roles {
			results = append(results, roleFromDiscordgo(r, guildID))
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return results, nil
}

// GetRole retrieves a role by ID.
func (c *Client) GetRole(ctx context.Context, guildID types.GuildID, roleID string) (*Role, error) {
	roles, err := c.ListRoles(ctx, guildID)
	if err != nil {
		return nil, err
	}

	for _, r := range roles {
		if r.ID == roleID {
			return r, nil
		}
	}

	return nil, fmt.Errorf("%w: role %s in guild %d", ErrRoleNotFound, roleID, guildID)
}

// CreateRole creates a new role in a guild.
func (c *Client) CreateRole(ctx context.Context, guildID types.GuildID, params RoleCreateParams) (*Role, error) {
	const op = "CreateRole"

	roleParams := &discordgo.RoleParams{
		Name:        params.Name,
		Color:       &params.Color,
		Permissions: &params.Permissions,
		Mentionable: &params.Mentionable,
	}

	var result *Role
	err := c.withRetry(ctx, op, func() error {
		r, err := c.session.GuildRoleCreate(
			guildIDToString(guildID),
			roleParams,
			c.requestOption(ctx)...,
		)
		if err != nil {
			return wrapError(op, err)
		}

		result = roleFromDiscordgo(r, guildID)
		return nil
	})

	if err != nil {
		return nil, err
	}

	c.logger.Debug("created role",
		"guild_id", guildID,
		"role_id", result.ID,
		"name", result.Name,
	)

	return result, nil
}

// EditRole modifies an existing role.
func (c *Client) EditRole(ctx context.Context, guildID types.GuildID, roleID string, params RoleEditParams) (*Role, error) {
	const op = "EditRole"

	roleParams := &discordgo.RoleParams{}

	if params.Name != nil {
		roleParams.Name = *params.Name
	}
	if params.Color != nil {
		roleParams.Color = params.Color
	}
	if params.Permissions != nil {
		roleParams.Permissions = params.Permissions
	}
	if params.Mentionable != nil {
		roleParams.Mentionable = params.Mentionable
	}

	var result *Role
	err := c.withRetry(ctx, op, func() error {
		r, err := c.session.GuildRoleEdit(
			guildIDToString(guildID),
			roleID,
			roleParams,
			c.requestOption(ctx)...,
		)
		if err != nil {
			return wrapError(op, err)
		}

		result = roleFromDiscordgo(r, guildID)
		return nil
	})

	if err != nil {
		return nil, err
	}

	c.logger.Debug("edited role",
		"guild_id", guildID,
		"role_id", roleID,
		"name", result.Name,
	)

	return result, nil
}

// DeleteRole deletes a role.
func (c *Client) DeleteRole(ctx context.Context, guildID types.GuildID, roleID string) error {
	const op = "DeleteRole"

	err := c.withRetry(ctx, op, func() error {
		return wrapError(op, c.session.GuildRoleDelete(
			guildIDToString(guildID),
			roleID,
			c.requestOption(ctx)...,
		))
	})

	if err != nil {
		return err
	}

	c.logger.Debug("deleted role",
		"guild_id", guildID,
		"role_id", roleID,
	)

	return nil
}

// FindRoleByName finds a role by name within a guild.
func (c *Client) FindRoleByName(ctx context.Context, guildID types.GuildID, name string) (*Role, error) {
	roles, err := c.ListRoles(ctx, guildID)
	if err != nil {
		return nil, err
	}

	for _, r := range roles {
		if r.Name == name {
			return r, nil
		}
	}

	return nil, fmt.Errorf("%w: role %q in guild %d", ErrRoleNotFound, name, guildID)
}

// ListRolesByPrefix returns all roles whose names start with the given prefix.
// This is useful for finding FSM pages which share a common prefix.
func (c *Client) ListRolesByPrefix(ctx context.Context, guildID types.GuildID, prefix string) ([]*Role, error) {
	roles, err := c.ListRoles(ctx, guildID)
	if err != nil {
		return nil, err
	}

	var filtered []*Role
	for _, r := range roles {
		if len(r.Name) >= len(prefix) && r.Name[:len(prefix)] == prefix {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}

// GetOrCreateRole gets a role by name or creates it if it doesn't exist.
func (c *Client) GetOrCreateRole(ctx context.Context, guildID types.GuildID, params RoleCreateParams) (*Role, error) {
	role, err := c.FindRoleByName(ctx, guildID, params.Name)
	if err == nil {
		return role, nil
	}

	if !IsNotFound(err) {
		// Check if it's a "role not found" error vs other errors
		if apiErr, ok := err.(*APIError); ok && apiErr.Err != ErrRoleNotFound {
			return nil, err
		}
	}

	return c.CreateRole(ctx, guildID, params)
}

// FSM provides convenience methods for Free Space Map operations using roles.
type FSM struct {
	client *Client
	prefix string
}

// FSMWithPrefix returns an FSM helper with the given role name prefix.
func (c *Client) FSMWithPrefix(prefix string) *FSM {
	return &FSM{client: c, prefix: prefix}
}

// ListPages returns all FSM pages (roles with the configured prefix).
func (fsm *FSM) ListPages(ctx context.Context, guildID types.GuildID) ([]*Role, error) {
	return fsm.client.ListRolesByPrefix(ctx, guildID, fsm.prefix)
}

// CreatePage creates a new FSM page.
func (fsm *FSM) CreatePage(ctx context.Context, guildID types.GuildID, pageID string, permissions int64, color int) (*Role, error) {
	return fsm.client.CreateRole(ctx, guildID, RoleCreateParams{
		Name:        fsm.prefix + pageID,
		Permissions: permissions,
		Color:       color,
	})
}

// UpdatePage updates an FSM page's permissions (trit storage).
func (fsm *FSM) UpdatePage(ctx context.Context, guildID types.GuildID, roleID string, permissions int64) (*Role, error) {
	return fsm.client.EditRole(ctx, guildID, roleID, RoleEditParams{
		Permissions: &permissions,
	})
}

// GetPage retrieves an FSM page by page ID suffix.
func (fsm *FSM) GetPage(ctx context.Context, guildID types.GuildID, pageID string) (*Role, error) {
	return fsm.client.FindRoleByName(ctx, guildID, fsm.prefix+pageID)
}

// DeletePage deletes an FSM page.
func (fsm *FSM) DeletePage(ctx context.Context, guildID types.GuildID, roleID string) error {
	return fsm.client.DeleteRole(ctx, guildID, roleID)
}
