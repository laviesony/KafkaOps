# KafkaOps User Guide

Welcome to **KafkaOps** – your local-first Kafka Dead Letter Queue (DLQ) remediation IDE. This guide walks you through how to inspect, fix, and replay failed Kafka messages.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Understanding the Interface](#understanding-the-interface)
3. [Viewing DLQ Messages](#viewing-dlq-messages)
4. [Inspecting a Message](#inspecting-a-message)
5. [Fix & Replay](#fix--replay)
6. [Bulk Operations (Pro)](#bulk-operations-pro)
7. [Keyboard Shortcuts](#keyboard-shortcuts)
8. [FAQ](#faq)

---

## Getting Started

When you open KafkaOps, you'll see the main dashboard displaying messages from your configured DLQ topic. The interface is divided into three main areas:

- **Message List** (left) – A scrollable list of all DLQ messages
- **Message Inspector** (top right) – Details about the selected message
- **Fix & Replay Editor** (bottom right) – Edit and replay messages

---

## Understanding the Interface

### Header
The header displays the KafkaOps logo and indicates you're running in **Local Mode** – meaning all data stays on your machine.

### Message List Panel
Shows all messages from the DLQ topic with:
- **Offset** – The unique position of the message in the topic
- **Topic** – The DLQ topic name
- **Timestamp** – When the message was produced
- **Payload Preview** – A snippet of the message content

### Pagination
At the bottom of the message list, use the **← →** arrows to navigate between pages. The current page and total pages are displayed.

---

## Viewing DLQ Messages

### Browsing Messages
1. Messages are loaded automatically when you open the app
2. Scroll through the list to browse messages
3. Use pagination controls to view more messages
4. The total message count is shown in the panel header

### Filtering (Coming Soon)
Future versions will support filtering by:
- Time range
- Error type
- Search text

---

## Inspecting a Message

Click on any message row to view its full details in the **Message Inspector** panel.

### Metadata Section
- **Topic** – The DLQ topic where this message resides
- **Partition** – Which partition the message is in
- **Offset** – The exact position in the partition
- **Timestamp** – When the message was originally produced
- **Key** – The message key (if present)

### Headers Section
Shows all Kafka headers attached to the message. Important headers include:

| Header | Description |
|--------|-------------|
| `X-Original-Topic` | The topic this message was meant for before failing |
| `X-Exception-Type` | The type of error that caused the failure |
| `X-Exception-Message` | The error message |
| `X-Retry-Count` | How many times replay was attempted |
| `X-Correlation-ID` | ID for tracing across systems |

### Decode Status
- ✅ **Valid JSON** – Message was successfully decoded
- ⚠️ **Decode Error** – Shows what went wrong during decoding

### Payload Preview
A formatted JSON view of the message content. This is the data you can edit and replay.

---

## Fix & Replay

The **Fix & Replay** feature allows you to edit a message and send it back to the original topic.

### Step 1: Select a Message
Click on a message in the list to load it into the editor.

### Step 2: Edit the Payload
The Monaco Editor shows the message as formatted JSON. You can:
- Fix invalid field values
- Add missing required fields
- Correct data types
- Remove problematic data

### Step 3: Validate
As you edit, the editor validates your JSON:
- ✅ **Green "Valid JSON"** badge – Ready to replay
- ❌ **Red error badge** – Fix the syntax errors shown

### Step 4: Replay
Click the **🚀 Replay Message** button to send the fixed message.

**What happens on replay:**
1. Your edited payload is sent to the original topic (from `X-Original-Topic` header)
2. Error headers (`X-Exception-*`) are automatically stripped
3. Tracing headers are preserved
4. You'll see a success message with the new offset

### Reset Button
Click **Reset** to restore the original payload if you want to start over.

---

## Bulk Operations (Pro)

> ⚠️ This feature requires a Pro license.

Bulk operations allow you to apply the same fix to multiple messages at once using **JSON Patch** (RFC 6902).

### How It Works

1. **Select Messages** – Choose multiple messages from the list
2. **Define Patch** – Write a JSON Patch to apply

```json
[
  { "op": "replace", "path": "/email", "value": "fixed@example.com" },
  { "op": "add", "path": "/fixedAt", "value": "2024-01-04" }
]
```

3. **Preview** – See what each message will look like after the patch
4. **Confirm & Execute** – Apply to all selected messages

### Supported Operations
- `replace` – Change a field's value
- `add` – Add a new field
- `remove` – Delete a field

---

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `↑` `↓` | Navigate between messages |
| `Enter` | Select highlighted message |
| `Ctrl+S` | Format JSON in editor |
| `Escape` | Clear selection |

---

## FAQ

### Why don't I see any messages?
- Check that your DLQ topic has messages
- Verify the backend is connected to Kafka (check logs)
- Refresh the page

### Where do replayed messages go?
Messages are sent to the topic specified in the `X-Original-Topic` header. If this header is missing, they go back to the DLQ topic.

### Are my messages sent to the cloud?
**No.** KafkaOps is local-first. All message data stays on your machine or within your VPC. No payloads are ever sent to external servers.

### What message formats are supported?
- **Avro** (with Schema Registry) – Automatically decoded
- **JSON** – Native support
- **Plain text** – Displayed as-is

### Can I undo a replay?
Once a message is replayed, it's produced to Kafka and cannot be undone through KafkaOps. However, if processing fails again, it will return to the DLQ.

### How do I switch DLQ topics?
Contact your administrator to update the `DLQ_TOPIC` configuration and restart the backend.

---

## Need Help?

If you encounter issues:
1. Check the browser console for errors
2. Review backend logs: `docker logs kafkaops-backend`
3. Ensure Kafka and Schema Registry are accessible

---

**Happy debugging!** 🚀
