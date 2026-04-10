package fsm

import (
	"context"
	"fmt"
	"log/slog"

	"discodb/discord"
	"discodb/types"
)

const SlotsPerPage = 40
const TritsPerPage = 40

type SlotState uint8

const (
	SlotFree SlotState = 0
	SlotDead SlotState = 1
	SlotLive SlotState = 2
)

type FSMPage struct {
	TableID   types.TableID
	PageID    uint32
	SlotData  []SlotState
	FreeCount uint32
}

func NewFSMPage(tableID types.TableID, pageID uint32) *FSMPage {
	return &FSMPage{
		TableID:   tableID,
		PageID:    pageID,
		SlotData:  make([]SlotState, SlotsPerPage),
		FreeCount: SlotsPerPage,
	}
}

func (p *FSMPage) Name() string {
	return fmt.Sprintf("fsm::%d::%d", p.TableID.Uint64(), p.PageID)
}

func (p *FSMPage) FindFreeSlot() (uint32, bool) {
	for i, state := range p.SlotData {
		if state == SlotFree {
			return uint32(i), true
		}
	}
	return 0, false
}

func (p *FSMPage) AllocateSlot(offset uint32) bool {
	if offset >= SlotsPerPage {
		return false
	}
	if p.SlotData[offset] != SlotFree {
		return false
	}
	p.SlotData[offset] = SlotLive
	p.FreeCount--
	return true
}

func (p *FSMPage) MarkDead(offset uint32) bool {
	if offset >= SlotsPerPage {
		return false
	}
	if p.SlotData[offset] != SlotLive {
		return false
	}
	p.SlotData[offset] = SlotDead
	return true
}

func (p *FSMPage) ReclaimSlot(offset uint32) bool {
	if offset >= SlotsPerPage {
		return false
	}
	if p.SlotData[offset] != SlotDead {
		return false
	}
	p.SlotData[offset] = SlotFree
	p.FreeCount++
	return true
}

func (p *FSMPage) EncodePermissions() int64 {
	var value int64
	for i := 0; i < SlotsPerPage; i++ {
		trit := int64(p.SlotData[i])
		value += trit * pow3Int(i)
	}
	return value
}

func pow3Int(n int) int64 {
	var result int64 = 1
	for i := 0; i < n; i++ {
		result *= 3
	}
	return result
}

func DecodePermissions(perms int64) []SlotState {
	slots := make([]SlotState, SlotsPerPage)
	temp := perms
	for i := 0; i < SlotsPerPage; i++ {
		slots[i] = SlotState(temp % 3)
		temp /= 3
	}
	return slots
}

type PageMetadata struct {
	FreeCount uint32
	Flags     uint8
}

func (m PageMetadata) EncodeColor() int {
	return int(m.FreeCount)<<8 | int(m.Flags)
}

func DecodeColor(color int) PageMetadata {
	return PageMetadata{
		FreeCount: uint32(color >> 8),
		Flags:     uint8(color & 0xFF),
	}
}

type Manager struct {
	client  *discord.Client
	guildID types.GuildID
	logger  *slog.Logger
	pages   map[types.TableID]map[uint32]*FSMPage
}

func NewManager(client *discord.Client, guildID types.GuildID, logger *slog.Logger) *Manager {
	return &Manager{
		client:  client,
		guildID: guildID,
		logger:  logger,
		pages:   make(map[types.TableID]map[uint32]*FSMPage),
	}
}

func (m *Manager) Discover(ctx context.Context) error {
	roles, err := m.client.ListRoles(ctx, m.guildID)
	if err != nil {
		return fmt.Errorf("list guild roles: %w", err)
	}

	for _, role := range roles {
		var tableID types.TableID
		var pageID uint32
		if _, err := fmt.Sscanf(role.Name, "fsm::%d::%d", &tableID, &pageID); err != nil {
			continue
		}

		if m.pages[tableID] == nil {
			m.pages[tableID] = make(map[uint32]*FSMPage)
		}

		perms := int64(role.Permissions)
		slots := DecodePermissions(perms)
		colorMeta := DecodeColor(role.Color)

		page := &FSMPage{
			TableID:   tableID,
			PageID:    pageID,
			SlotData:  slots,
			FreeCount: colorMeta.FreeCount,
		}

		m.pages[tableID][pageID] = page
		m.logger.Debug("discovered FSM page",
			slog.String("table_id", tableID.String()),
			slog.Uint64("page_id", uint64(pageID)),
			slog.Uint64("free_count", uint64(page.FreeCount)),
		)
	}

	m.logger.Info("FSM discovery complete",
		slog.Int("table_count", len(m.pages)),
	)

	return nil
}

func (m *Manager) AllocateSlot(ctx context.Context, tableID types.TableID) (uint32, uint32, error) {
	page, offset, err := m.findPageWithFreeSlot(tableID)
	if err != nil {
		return 0, 0, err
	}

	if !page.AllocateSlot(offset) {
		return 0, 0, fmt.Errorf("failed to allocate slot on page")
	}

	if err := m.updatePageRole(ctx, page); err != nil {
		page.SlotData[offset] = SlotFree
		page.FreeCount++
		return 0, 0, fmt.Errorf("update page role: %w", err)
	}

	m.logger.Debug("allocated slot",
		slog.String("table_id", tableID.String()),
		slog.Uint64("page_id", uint64(page.PageID)),
		slog.Uint64("offset", uint64(offset)),
	)

	return page.PageID, offset, nil
}

func (m *Manager) findPageWithFreeSlot(tableID types.TableID) (*FSMPage, uint32, error) {
	tablePages, ok := m.pages[tableID]
	if !ok || len(tablePages) == 0 {
		newPage := m.createNewPage(tableID, 0)
		return newPage, 0, nil
	}

	for _, page := range tablePages {
		if offset, ok := page.FindFreeSlot(); ok {
			return page, offset, nil
		}
	}

	pageID := uint32(len(tablePages))
	newPage := m.createNewPage(tableID, pageID)
	return newPage, 0, nil
}

func (m *Manager) createNewPage(tableID types.TableID, pageID uint32) *FSMPage {
	page := NewFSMPage(tableID, pageID)
	if m.pages[tableID] == nil {
		m.pages[tableID] = make(map[uint32]*FSMPage)
	}
	m.pages[tableID][pageID] = page
	return page
}

func (m *Manager) updatePageRole(ctx context.Context, page *FSMPage) error {
	roleName := page.Name()
	perms := page.EncodePermissions()
	color := PageMetadata{FreeCount: page.FreeCount}.EncodeColor()

	roles, err := m.client.ListRoles(ctx, m.guildID)
	if err != nil {
		return fmt.Errorf("list roles for update: %w", err)
	}

	var existingRole *discord.Role
	for _, r := range roles {
		if r.Name == roleName {
			existingRole = r
			break
		}
	}

	if existingRole != nil {
		_, err = m.client.EditRole(ctx, m.guildID, existingRole.ID, discord.RoleEditParams{
			Permissions: &perms,
			Color:       &color,
		})
		if err != nil {
			return fmt.Errorf("edit role: %w", err)
		}
	} else {
		_, err = m.client.CreateRole(ctx, m.guildID, discord.RoleCreateParams{
			Name:        roleName,
			Permissions: perms,
			Color:       color,
		})
		if err != nil {
			return fmt.Errorf("create role: %w", err)
		}
	}

	return nil
}

func (m *Manager) MarkDead(tableID types.TableID, pageID uint32, offset uint32) error {
	page, ok := m.pages[tableID][pageID]
	if !ok {
		return fmt.Errorf("page not found: table=%s page=%d", tableID.String(), pageID)
	}

	if !page.MarkDead(offset) {
		return fmt.Errorf("slot not live")
	}

	return nil
}

func (m *Manager) ReclaimSlot(ctx context.Context, tableID types.TableID, pageID uint32, offset uint32) error {
	page, ok := m.pages[tableID][pageID]
	if !ok {
		return fmt.Errorf("page not found: table=%s page=%d", tableID.String(), pageID)
	}

	if !page.ReclaimSlot(offset) {
		return fmt.Errorf("slot not dead")
	}

	if err := m.updatePageRole(ctx, page); err != nil {
		return fmt.Errorf("update page role: %w", err)
	}

	return nil
}

func (m *Manager) GetGlobalSlotID(tableID types.TableID, pageID uint32, offset uint32) uint64 {
	return uint64(pageID)*SlotsPerPage + uint64(offset)
}

func (m *Manager) ResolveSlot(globalSlotID uint64) (types.TableID, uint32, uint32) {
	pageID := globalSlotID / SlotsPerPage
	offset := globalSlotID % SlotsPerPage
	return 0, uint32(pageID), uint32(offset)
}
