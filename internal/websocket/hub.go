// Package websocket implements the pub/sub broadcast Hub that fans out
// events to subscribed clients without ever blocking the ingestion/DB
// pipeline (see CLAUDE.md rule 4).
package websocket
