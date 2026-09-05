import asyncio
import logging
import re
import sys
import datetime
import time
import random
import os
import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    FloodWaitError,
    UserAlreadyParticipantError,
    InviteRequestSentError,
    MessageNotModifiedError
)
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest, GetFullChannelRequest
from telethon.tl.functions.account import UpdateStatusRequest
from telethon.tl.functions.account import GetAuthorizationsRequest, ResetAuthorizationRequest
from telethon.tl.functions.phone import LeaveGroupCallRequest, JoinGroupCallRequest
from telethon.tl.types import DataJSON, InputGroupCall
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# ===================== CREDENTIALS =====================
API_ID = 36835223
API_HASH = "dd9df6119c26af3fdd05448eeba72581"
BOT_TOKEN = "mongodb+srv://Elevenyts:Elevenyts@cluster0.vuyc1u2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_URI = "mongodb+srv://Elevenyts:Elevenyts@cluster0.vuyc1u2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"


MAIN_OWNER_ID = 8785465683  
OWNER_IDS =  [8785465683]
KEEP_ALIVE_INTERVAL = 300
START_TIME = datetime.datetime.now()

active_clients = {}
task_states = {}
last_otp = {}
session_states = {}
vc_tasks = {}
vc_target = None

# ===================== MongoDB =====================
try:
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    db = mongo_client['tg_manager_bot']
    col_sessions = db['sessions']
    col_history = db['history']
    col_approved = db['approved_users']
    col_2fa = db['2fa_passwords']
    logger.info("MongoDB Connected")
except Exception as e:
    logger.error(f"MongoDB Error: {e}")
    sys.exit(1)

# ===================== Helper Functions =====================
async def save_2fa(phone: str, password: str) -> bool:
    try:
        await col_2fa.update_one(
            {'phone': phone},
            {'$set': {'phone': phone, 'password': password, 'updated_at': datetime.datetime.utcnow()}},
            upsert=True
        )
        return True
    except:
        return False

async def get_2fa(phone: str) -> Optional[str]:
    doc = await col_2fa.find_one({'phone': phone})
    return doc.get('password') if doc else None

async def approve_user(user_id: int, approved_by: int, is_admin: bool = False) -> bool:
    try:
        await col_approved.update_one(
            {'user_id': user_id},
            {'$set': {
                'user_id': user_id,
                'approved_by': approved_by,
                'approved_at': datetime.datetime.utcnow(),
                'is_approved': True,
                'is_admin': is_admin
            }},
            upsert=True
        )
        return True
    except:
        return False

async def unapprove_user(user_id: int) -> bool:
    try:
        result = await col_approved.delete_one({'user_id': user_id})
        return result.deleted_count > 0
    except:
        return False

async def is_user_approved(user_id: int) -> bool:
    if user_id in OWNER_IDS:
        return True
    try:
        doc = await col_approved.find_one({'user_id': user_id, 'is_approved': True})
        return doc is not None
    except Exception as e:
        logger.error(f"is_user_approved error: {e}")
        return False

async def can_approve_users(user_id: int) -> bool:
    if user_id in OWNER_IDS:
        return True
    doc = await col_approved.find_one({'user_id': user_id, 'is_approved': True})
    return doc.get('is_admin', False) if doc else False

async def is_user_admin(user_id: int) -> bool:
    if user_id in OWNER_IDS:
        return True
    doc = await col_approved.find_one({'user_id': user_id, 'is_approved': True})
    return doc.get('is_admin', False) if doc else False

async def is_main_owner(user_id: int) -> bool:
    return user_id == MAIN_OWNER_ID or user_id in OWNER_IDS

async def get_approved_users() -> List[dict]:
    cursor = col_approved.find({'is_approved': True})
    users = []
    async for doc in cursor:
        users.append({
            'user_id': doc['user_id'],
            'approved_by': doc.get('approved_by'),
            'approved_at': doc.get('approved_at'),
            'is_admin': doc.get('is_admin', False)
        })
    return users

async def get_today_joins():
    today = datetime.datetime.now().date()
    count = 0
    async for doc in col_history.find({
        'action': 'JOIN',
        'timestamp': {'$gte': datetime.datetime.combine(today, datetime.time.min)}
    }):
        count += 1
    return count

def get_uptime():
    diff = datetime.datetime.now() - START_TIME
    minutes = int(diff.total_seconds() / 60)
    if minutes < 60:
        return f"{minutes}m"
    elif minutes < 1440:
        return f"{minutes // 60}h {minutes % 60}m"
    else:
        return f"{minutes // 1440}d"

async def get_all_sessions():
    cursor = col_sessions.find({})
    sessions = {}
    async for doc in cursor:
        sessions[doc['phone']] = doc['session']
    return sessions

async def get_all_phones() -> List[str]:
    return list(active_clients.keys())

async def add_session(phone, session_str):
    await col_sessions.update_one(
        {'phone': phone},
        {'$set': {'phone': phone, 'session': session_str, 'updated_at': datetime.datetime.utcnow()}},
        upsert=True
    )

async def delete_session_and_2fa(phone: str):
    await col_sessions.delete_one({'phone': phone})
    await col_2fa.delete_one({'phone': phone})

async def log_activity(phone, action, target, status):
    try:
        await col_history.insert_one({
            'phone': phone,
            'action': action,
            'target': target,
            'status': status,
            'timestamp': datetime.datetime.utcnow()
        })
    except:
        pass

# ---------- keep_alive and OTP ----------
async def keep_alive(client, phone):
    @client.on(events.NewMessage)
    async def otp_listener(event):
        try:
            text = event.raw_text
            if text:
                match = re.search(r'\b(\d{5,6})\b', text)
                if match:
                    code = match.group(1)
                    last_otp[phone] = code
                    logger.info(f"Captured OTP for {phone}: {code}")
        except Exception as e:
            logger.error(f"OTP listener error for {phone}: {e}")

    while True:
        try:
            await client(UpdateStatusRequest(offline=False))
            await asyncio.sleep(2)
            await client.get_me()
        except:
            pass
        await asyncio.sleep(KEEP_ALIVE_INTERVAL)

async def start_saved_clients():
    sessions = await get_all_sessions()
    logger.info(f"Loading {len(sessions)} sessions...")
    for phone, session_str in sessions.items():
        try:
            client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                active_clients[phone] = client
                asyncio.create_task(keep_alive(client, phone))
                logger.info(f"Connected: {phone}")
        except Exception as e:
            logger.error(f"Failed to connect {phone}: {e}")
    logger.info(f"Connected {len(active_clients)} accounts")

def parse_target(link: str):
    link = link.strip()
    username_match = re.search(r"(?:t\.me/|@)([a-zA-Z0-9_]{5,32})", link)
    if username_match and "joinchat" not in link:
        return ("public", username_match.group(1))
    invite_match = re.search(r"t\.me/(?:\+|joinchat/)([a-zA-Z0-9_-]+)", link)
    if invite_match:
        return ("private", invite_match.group(1))
    return (None, None)

# ===================== VOICE CHAT FUNCTIONS =====================
async def join_voice_chat(client: TelegramClient, target: str, leave_after: bool = False) -> tuple:
    link_type, identifier = parse_target(target)
    if not link_type:
        return False, None

    entity = None

    try:
        if link_type == 'private':
            try:
                updates = await client(ImportChatInviteRequest(identifier))
                if updates and updates.chats:
                    entity = updates.chats[0]
                else:
                    invite = await client(CheckChatInviteRequest(identifier))
                    if invite and hasattr(invite, 'chat'):
                        entity = invite.chat
                    else:
                        if hasattr(invite, 'title'):
                            title = invite.title
                            async for dialog in client.iter_dialogs():
                                if dialog.title == title:
                                    entity = dialog.entity
                                    break
            except UserAlreadyParticipantError:
                try:
                    invite = await client(CheckChatInviteRequest(identifier))
                    if invite and hasattr(invite, 'chat'):
                        entity = invite.chat
                    else:
                        if hasattr(invite, 'title'):
                            title = invite.title
                            async for dialog in client.iter_dialogs():
                                if dialog.title == title:
                                    entity = dialog.entity
                                    break
                except:
                    try:
                        entity = await client.get_entity(f"https://t.me/+{identifier}")
                    except:
                        pass
            except Exception as e:
                logger.error(f"Private invite error: {e}")
                return False, None
        else:
            try:
                await client(JoinChannelRequest(identifier))
            except UserAlreadyParticipantError:
                pass
            except Exception as e:
                logger.error(f"Join channel error: {e}")
            try:
                entity = await client.get_entity(identifier)
            except:
                try:
                    entity = await client.get_entity(f"@{identifier}")
                except:
                    pass

        if not entity:
            logger.error(f"Could not resolve entity for {target}")
            return False, None

        try:
            full_chat = await client(GetFullChannelRequest(entity))
        except Exception as e:
            logger.error(f"GetFullChannel error: {e}")
            return False, None

        if not full_chat or not full_chat.full_chat.call:
            logger.info(f"No active voice chat found for {target}")
            return False, None

        call_obj = full_chat.full_chat.call

        try:
            my_ssrc = random.randint(10000, 99999999)
            params = DataJSON(data=json.dumps({"min_version": 2, "ssrc": my_ssrc, "muted": True}))
            await client(JoinGroupCallRequest(
                call=call_obj,
                join_as=await client.get_input_entity('me'),
                params=params,
                muted=True
            ))
        except Exception as e:
            if "SSRC" in str(e):
                my_ssrc = random.randint(10000, 99999999)
                params = DataJSON(data=json.dumps({"min_version": 2, "ssrc": my_ssrc, "muted": True}))
                await client(JoinGroupCallRequest(
                    call=call_obj,
                    join_as=await client.get_input_entity('me'),
                    params=params,
                    muted=True
                ))
            else:
                logger.error(f"JoinGroupCall error: {e}")
                raise e

        if leave_after:
            await client(LeaveGroupCallRequest(call_obj))
            return True, None

        return True, call_obj

    except Exception as e:
        logger.error(f"join_voice_chat error for {target}: {e}")
        return False, None

async def stay_in_voice_chat(client: TelegramClient, target: str, refresh_interval: float = 15.0):
    while True:
        try:
            success, call_obj = await join_voice_chat(client, target, leave_after=False)
            if success and call_obj:
                logger.info(f"✅ Joined VC. Staying for {refresh_interval}s...")
                await asyncio.sleep(refresh_interval)
                logger.info("🔄 Refreshing...")
                try:
                    await client(LeaveGroupCallRequest(call_obj))
                except Exception as e:
                    logger.warning(f"Leave error (ignored): {e}")
                await asyncio.sleep(2)
            else:
                logger.warning("❌ Failed to join VC. Retrying in 5s...")
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error in stay loop: {e}")
            await asyncio.sleep(5)

async def start_vc_for_phone(client: TelegramClient, phone: str, target: str, refresh_interval: float = 15.0):
    try:
        await stay_in_voice_chat(client, target, refresh_interval)
    except asyncio.CancelledError:
        logger.info(f"VC task for {phone} cancelled.")
    except Exception as e:
        logger.error(f"VC task for {phone} crashed: {e}")
    finally:
        if phone in vc_tasks:
            del vc_tasks[phone]

async def start_vc_for_phones(event, target: str, phones: List[str], refresh_interval: float = 15.0):
    global vc_target
    if not phones:
        await event.respond("❌ No accounts selected!")
        return

    for phone in phones:
        if phone in vc_tasks:
            vc_tasks[phone].cancel()
            del vc_tasks[phone]

    vc_target = target

    started = 0
    for phone in phones:
        client = active_clients.get(phone)
        if not client:
            continue
        task = asyncio.create_task(start_vc_for_phone(client, phone, target, refresh_interval))
        vc_tasks[phone] = task
        started += 1
        await asyncio.sleep(0.2)

    await event.respond(f"✅ Started VC for {started} accounts.\nTarget: {target}")

async def stop_all_vc(event):
    global vc_target
    if not vc_tasks:
        await event.respond("ℹ️ No active VC sessions.")
        return
    count = len(vc_tasks)
    for phone, task in list(vc_tasks.items()):
        task.cancel()
        del vc_tasks[phone]
    vc_target = None
    await event.respond(f"⏹️ Stopped VC for {count} accounts.")

# ===================== JOIN, LEAVE, VIEWS logic (unchanged) =====================
async def execute_join(event, task_data, count_limit):
    links_data = task_data['links_data']
    delay = task_data['delay']
    all_phones = list(active_clients.keys())
    selected = all_phones[:count_limit]
    
    msg = await event.respond(f"🚀 **JOINING** {len(links_data)} links with {len(selected)} accounts...")
    
    success = 0
    failed = 0
    already = 0
    
    total_ops = len(links_data) * len(selected)
    current_op = 0
    
    for link_idx, link_info in enumerate(links_data, 1):
        link_type = link_info['type']
        target = link_info['target']
        original_link = link_info['link']
        
        for acc_idx, phone in enumerate(selected):
            current_op += 1
            client = active_clients[phone]
            
            try:
                if link_type == 'public':
                    await client(JoinChannelRequest(target))
                else:
                    await client(ImportChatInviteRequest(target))
                success += 1
                await log_activity(phone, "JOIN", original_link, "Success")
            except UserAlreadyParticipantError:
                already += 1
                success += 1
            except InviteRequestSentError:
                success += 1
            except FloodWaitError as e:
                wait_time = e.seconds if e.seconds < 30 else 30
                await asyncio.sleep(wait_time)
                try:
                    if link_type == 'public':
                        await client(JoinChannelRequest(target))
                    else:
                        await client(ImportChatInviteRequest(target))
                    success += 1
                except:
                    failed += 1
            except:
                failed += 1
            
            percent = int((current_op / total_ops) * 100)
            
            status_text = f"""🚀 **JOIN PROGRESS**
━━━━━━━━━━━━━━━━━━
📊 `{percent}%` Complete
✅ Success: `{success}`
⚠️ Already: `{already}`
❌ Failed: `{failed}`
━━━━━━━━━━━━━━━━━━
📌 Link: `{original_link[:30]}`
👤 Account: `{phone}`"""
            
            try:
                await msg.edit(status_text, parse_mode='md')
            except:
                pass
            await asyncio.sleep(delay)
    
    final_text = f"""✅ **JOIN COMPLETE!**
━━━━━━━━━━━━━━━━━━
📊 **FINAL STATS**
✅ Joined: `{success - already}`
⚠️ Already: `{already}`
❌ Failed: `{failed}`
━━━━━━━━━━━━━━━━━━
📌 Links: `{len(links_data)}`
👥 Accounts: `{len(selected)}`"""
    
    await msg.edit(final_text, parse_mode='md')
    await event.respond("✅ Task Complete!", buttons=[[Button.inline("◀️ MAIN MENU", b"back")]])

async def execute_leave_specific(event, link):
    link_type, target = parse_target(link)
    if not link_type:
        return await event.respond("❌ Invalid link!", buttons=[[Button.inline("◀️ BACK", b"back")]])
    
    msg = await event.respond(f"📤 **LEAVING:** {link}")
    count = 0
    
    for phone, client in active_clients.items():
        try:
            if link_type == 'public':
                await client(LeaveChannelRequest(target))
            else:
                entity = await client.get_entity(link)
                await client.delete_dialog(entity)
            count += 1
            await msg.edit(f"📤 **LEAVING...**\n✅ Left: `{count}`", parse_mode='md')
        except:
            pass
        await asyncio.sleep(1)
    
    await msg.edit(f"✅ **LEFT FROM** `{count}` **ACCOUNTS**", parse_mode='md')
    await event.respond("✅ Task Complete!", buttons=[[Button.inline("◀️ MAIN MENU", b"back")]])

async def execute_leave_all(event):
    msg = await event.respond("⚠️ **LEAVING ALL CHANNELS...**")
    total = 0
    
    for phone, client in active_clients.items():
        try:
            async for dialog in client.iter_dialogs():
                if dialog.is_channel or dialog.is_group:
                    try:
                        await client.delete_dialog(dialog.entity)
                        total += 1
                        await msg.edit(f"⚠️ **LEAVING...**\n✅ Left: `{total}`", parse_mode='md')
                        await asyncio.sleep(0.5)
                    except:
                        pass
        except:
            pass
    
    await msg.edit(f"✅ **LEFT FROM** `{total}` **CHATS**", parse_mode='md')
    await event.respond("✅ Task Complete!", buttons=[[Button.inline("◀️ MAIN MENU", b"back")]])

async def execute_views(event, post_link, count_limit):
    channel_username = None
    message_id = None
    chat_id = None
    
    public_match = re.search(r"t\.me/([a-zA-Z0-9_]+)/(\d+)", post_link)
    if public_match:
        channel_username = public_match.group(1)
        message_id = int(public_match.group(2))
    
    private_match = re.search(r"t\.me/c/(\d+)/(\d+)", post_link)
    if private_match:
        channel_id = int(private_match.group(1))
        message_id = int(private_match.group(2))
        chat_id = int(f"-100{channel_id}")
    
    if not channel_username and not chat_id:
        return await event.respond("❌ Invalid post link!", buttons=[[Button.inline("◀️ BACK", b"back")]])
    
    all_phones = list(active_clients.keys())
    selected = all_phones[:count_limit] if count_limit > 0 else all_phones
    
    if not selected:
        return await event.respond("❌ No active accounts!", buttons=[[Button.inline("◀️ BACK", b"back")]])
    
    msg = await event.respond(f"👁️ SENDING REAL VIEWS...\n📌 {post_link[:50]}...\n👥 Accounts: {len(selected)}")
    
    success = 0
    failed = 0
    not_member = 0
    
    for acc_idx, phone in enumerate(selected):
        client = active_clients[phone]
        
        try:
            if chat_id:
                try:
                    message = await client.get_messages(chat_id, ids=message_id)
                    if message:
                        success += 1
                except:
                    not_member += 1
            else:
                try:
                    entity = await client.get_entity(channel_username)
                    message = await client.get_messages(entity, ids=message_id)
                    if message:
                        success += 1
                except:
                    try:
                        await client(JoinChannelRequest(channel_username))
                        await asyncio.sleep(1)
                        entity = await client.get_entity(channel_username)
                        message = await client.get_messages(entity, ids=message_id)
                        if message:
                            success += 1
                        else:
                            failed += 1
                    except:
                        failed += 1
        except:
            failed += 1
        
        percent = int(((acc_idx + 1) / len(selected)) * 100)
        
        try:
            await msg.edit(f"👁️ VIEWS: {percent}%\n✅ Success: {success}\n❌ Failed: {failed}\n🚫 Not Member: {not_member}")
        except:
            pass
        
        await asyncio.sleep(0.3)
    
    await msg.edit(f"✅ VIEWS SENT!\n\n👁️ Success: {success}\n❌ Failed: {failed}\n🚫 Not Member: {not_member}")
    await event.respond("✅ Task Complete!", buttons=[[Button.inline("◀️ MAIN MENU", b"back")]])

async def execute_views_batch(event, links_text, count_per_post):
    links = [l.strip() for l in links_text.strip().splitlines() if l.strip()]
    
    if not links:
        return await event.respond("❌ No valid links!", buttons=[[Button.inline("◀️ BACK", b"back")]])
    
    if len(links) > 20:
        return await event.respond("⚠️ Max 20 posts at once!", buttons=[[Button.inline("◀️ BACK", b"back")]])
    
    msg = await event.respond(f"👁️ PROCESSING {len(links)} POSTS...")
    total_views = 0
    
    for idx, link in enumerate(links, 1):
        await msg.edit(f"📌 Processing post {idx}/{len(links)}...")
        
        public_match = re.search(r"t\.me/([a-zA-Z0-9_]+)/(\d+)", link)
        private_match = re.search(r"t\.me/c/(\d+)/(\d+)", link)
        
        if not public_match and not private_match:
            continue
        
        if public_match:
            channel_username = public_match.group(1)
            message_id = int(public_match.group(2))
            for phone, client in active_clients.items():
                try:
                    entity = await client.get_entity(channel_username)
                    await client.get_messages(entity, ids=message_id)
                    total_views += 1
                except:
                    pass
                await asyncio.sleep(0.3)
        else:
            channel_id = int(private_match.group(1))
            message_id = int(private_match.group(2))
            chat_id = int(f"-100{channel_id}")
            for phone, client in active_clients.items():
                try:
                    await client.get_messages(chat_id, ids=message_id)
                    total_views += 1
                except:
                    pass
                await asyncio.sleep(0.3)
    
    await msg.edit(f"✅ BATCH COMPLETE!\n\n👁️ Total Views Sent: {total_views}")
    await event.respond("✅ Task Complete!", buttons=[[Button.inline("◀️ MAIN MENU", b"back")]])

# ============================================================
#  MANAGE SYSTEM (with pagination, OTP retrieval, session termination)
# ============================================================
manage_states = {}

async def show_manage_menu(event, page: int = 0, edit_msg=None):
    user_id = event.sender_id
    if not await is_main_owner(user_id):
        if edit_msg:
            await edit_msg.edit("⛔ Owner only!", buttons=[[Button.inline("◀️ BACK", b"back")]])
        else:
            await event.respond("⛔ Owner only!", buttons=[[Button.inline("◀️ BACK", b"back")]])
        return

    try:
        phones = await get_all_phones()
    except Exception as e:
        logger.error(f"get_all_phones error: {e}")
        if edit_msg:
            await edit_msg.edit(f"❌ Database error: {e}", buttons=[[Button.inline("◀️ BACK", b"back")]])
        else:
            await event.respond(f"❌ Database error: {e}", buttons=[[Button.inline("◀️ BACK", b"back")]])
        return

    if not phones:
        menu = "📱 No active accounts."
        buttons = [[Button.inline("◀️ BACK", b"back")]]
        if edit_msg:
            await edit_msg.edit(menu, buttons=buttons, parse_mode='md')
        else:
            await event.respond(menu, buttons=buttons, parse_mode='md')
        return

    per_page = 5
    total_pages = (len(phones) + per_page - 1) // per_page
    if page < 0: page = 0
    if page >= total_pages: page = total_pages - 1

    manage_states[user_id] = {'phones': phones, 'page': page}

    start = page * per_page
    end = min(start + per_page, len(phones))
    page_phones = phones[start:end]

    try:
        cursor = col_2fa.find({'phone': {'$in': page_phones}}, {'phone': 1, 'password': 1, '_id': 0})
        fa_dict = {}
        async for doc in cursor:
            fa_dict[doc['phone']] = doc.get('password', '')
    except Exception as e:
        logger.error(f"2FA fetch error: {e}")
        fa_dict = {}

    menu = f"📱 MANAGER PANEL (Page {page+1}/{total_pages})\n"
    menu += "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    for idx, phone in enumerate(page_phones, start=start+1):
        saved_2fa = fa_dict.get(phone, '')
        status = "🔐 2FA: " + (f"`{saved_2fa[:10]}...`" if saved_2fa else "`No 2FA`")
        menu += f"{idx}. 📱 `{phone}`\n   └ {status}\n\n"

    buttons = []
    nav = []
    if page > 0:
        nav.append(Button.inline("⬅️ PREV", f"mg_page_{page-1}".encode()))
    if page < total_pages - 1:
        nav.append(Button.inline("NEXT ➡️", f"mg_page_{page+1}".encode()))
    if nav:
        buttons.append(nav)

    for phone in page_phones:
        safe_phone = phone.replace('+', '')
        buttons.append([Button.inline(f"📱 {phone}", f"mg_{safe_phone}".encode())])

    buttons.append([Button.inline("◀️ BACK", b"back")])

    if edit_msg:
        await edit_msg.edit(menu, buttons=buttons, parse_mode='md')
    else:
        await event.respond(menu, buttons=buttons, parse_mode='md')

async def show_phone_actions(event, phone: str, edit_msg=None):
    saved_2fa = await get_2fa(phone)
    menu = f"""📱 PHONE: `{phone}`
━━━━━━━━━━━━━━━━━━━━━━
🔐 2FA: `{saved_2fa if saved_2fa else 'No 2FA'}`
━━━━━━━━━━━━━━━━━━━━━━

Select an action:"""
    buttons = [
        [Button.inline("📱 GET OTP", f"get_otp_{phone}".encode())],
        [Button.inline("🔴 TERMINATE SESSIONS", f"term_sess_{phone}".encode())],
        [Button.inline("🗑️ DELETE SESSION", f"del_sess_{phone}".encode())],
        [Button.inline("◀️ BACK TO MANAGE", b"manage_menu")]
    ]
    if edit_msg:
        await edit_msg.edit(menu, buttons=buttons, parse_mode='md')
    else:
        await event.respond(menu, buttons=buttons, parse_mode='md')

# ---------- OTP retrieval ----------
async def fetch_and_show_otp(event, phone: str):
    client = active_clients.get(phone)
    if not client:
        doc = await col_sessions.find_one({'phone': phone})
        if not doc or not doc.get('session'):
            await event.edit("❌ Session not found! Please re-add this account.", buttons=[[Button.inline("◀️ BACK", b"manage_menu")]])
            return
        try:
            client = TelegramClient(StringSession(doc['session']), API_ID, API_HASH)
            await client.connect()
            if not await client.is_user_authorized():
                await event.edit("❌ Session expired. Please re-add this account.", buttons=[[Button.inline("◀️ BACK", b"manage_menu")]])
                return
            active_clients[phone] = client
            asyncio.create_task(keep_alive(client, phone))
        except Exception as e:
            await event.edit(f"❌ Failed to connect: {e}", buttons=[[Button.inline("◀️ BACK", b"manage_menu")]])
            return

    if phone in last_otp:
        code = last_otp[phone]
        await event.edit(f"✅ **OTP Code:** `{code}`\n\n(Retrieved from recent messages)", 
                         buttons=[[Button.inline("◀️ BACK TO PHONE", f"mg_{phone.replace('+','')}".encode())]], 
                         parse_mode='md')
        return

    try:
        entity = await client.get_entity('telegram')
    except:
        entity = None

    if entity:
        try:
            messages = await client.get_messages(entity, limit=10)
            for msg in messages:
                if msg.text:
                    match = re.search(r'\b(\d{5,6})\b', msg.text)
                    if match:
                        code = match.group(1)
                        last_otp[phone] = code
                        await event.edit(f"✅ **OTP Code:** `{code}`\n\n(Retrieved from recent messages)", 
                                         buttons=[[Button.inline("◀️ BACK TO PHONE", f"mg_{phone.replace('+','')}".encode())]], 
                                         parse_mode='md')
                        return
        except:
            pass

    try:
        async for dialog in client.iter_dialogs():
            if dialog.is_user:
                messages = await client.get_messages(dialog.entity, limit=5)
                for msg in messages:
                    if msg.text:
                        match = re.search(r'\b(\d{5,6})\b', msg.text)
                        if match:
                            code = match.group(1)
                            last_otp[phone] = code
                            await event.edit(f"✅ **OTP Code:** `{code}`\n\n(Retrieved from recent messages)", 
                                             buttons=[[Button.inline("◀️ BACK TO PHONE", f"mg_{phone.replace('+','')}".encode())]], 
                                             parse_mode='md')
                            return
    except:
        pass

    await event.edit("❌ No recent OTP found.\n\nThe bot checks recent messages for 5-6 digit codes. Make sure an OTP has been sent to this account's Telegram app recently.",
                     buttons=[[Button.inline("◀️ BACK", b"manage_menu")]])

# ========== Session termination functions ==========
async def show_sessions_list(event, phone: str, edit_msg=None):
    client = active_clients.get(phone)
    if not client:
        doc = await col_sessions.find_one({'phone': phone})
        if not doc or not doc.get('session'):
            await (edit_msg.edit if edit_msg else event.respond)(
                "❌ Session not found! Please re-add this account.",
                buttons=[[Button.inline("◀️ BACK", b"manage_menu")]]
            )
            return
        try:
            client = TelegramClient(StringSession(doc['session']), API_ID, API_HASH)
            await client.connect()
            if not await client.is_user_authorized():
                await (edit_msg.edit if edit_msg else event.respond)(
                    "❌ Session expired. Please re-add this account.",
                    buttons=[[Button.inline("◀️ BACK", b"manage_menu")]]
                )
                return
            active_clients[phone] = client
            asyncio.create_task(keep_alive(client, phone))
        except Exception as e:
            await (edit_msg.edit if edit_msg else event.respond)(
                f"❌ Failed to connect: {e}",
                buttons=[[Button.inline("◀️ BACK", b"manage_menu")]]
            )
            return

    try:
        auths = await client(GetAuthorizationsRequest())
        sessions = auths.authorizations
    except Exception as e:
        await (edit_msg.edit if edit_msg else event.respond)(
            f"❌ Failed to fetch sessions: {e}",
            buttons=[[Button.inline("◀️ BACK", b"manage_menu")]]
        )
        return

    if not sessions:
        await (edit_msg.edit if edit_msg else event.respond)(
            "No active sessions found.",
            buttons=[[Button.inline("◀️ BACK", b"manage_menu")]]
        )
        return

    menu = f"📱 **Active Sessions for {phone}**\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
    buttons = []
    for auth in sessions:
        if auth.current:
            menu += f"✅ **CURRENT SESSION**\n"
        else:
            menu += f"🔹 **{auth.device_model}** ({auth.platform})\n"
        menu += f"   App: {auth.app_name}\n"
        location = getattr(auth, 'country', None) or getattr(auth, 'region', 'Unknown')
        menu += f"   IP: {auth.ip} ({location})\n"
        menu += f"   Active: {auth.date_active.strftime('%Y-%m-%d %H:%M')}\n"
        menu += f"   Created: {auth.date_created.strftime('%Y-%m-%d %H:%M')}\n"
        if not auth.current:
            buttons.append([Button.inline(f"❌ Terminate {auth.device_model}",
                                          f"kill_sess|{phone}|{auth.hash}".encode())])
        menu += "\n"

    buttons.append([Button.inline("◀️ BACK TO PHONE", f"mg_{phone.replace('+','')}".encode())])

    try:
        if edit_msg:
            await edit_msg.edit(menu, buttons=buttons, parse_mode='md')
        else:
            await event.respond(menu, buttons=buttons, parse_mode='md')
    except Exception as e:
        logger.error(f"Error showing sessions list: {e}")
        await (edit_msg.edit if edit_msg else event.respond)(
            f"❌ Error displaying sessions: {e}",
            buttons=[[Button.inline("◀️ BACK", b"manage_menu")]]
        )

async def terminate_session(event, phone: str, session_hash: int):
    client = active_clients.get(phone)
    if not client:
        doc = await col_sessions.find_one({'phone': phone})
        if doc and doc.get('session'):
            try:
                client = TelegramClient(StringSession(doc['session']), API_ID, API_HASH)
                await client.connect()
                if await client.is_user_authorized():
                    active_clients[phone] = client
                    asyncio.create_task(keep_alive(client, phone))
                else:
                    await event.edit("❌ Session expired. Please re-add this account.",
                                     buttons=[[Button.inline("◀️ BACK", b"manage_menu")]])
                    return
            except Exception as e:
                await event.edit(f"❌ Failed to reconnect: {e}",
                                 buttons=[[Button.inline("◀️ BACK", b"manage_menu")]])
                return
        else:
            await event.edit("❌ No session found for this phone.",
                             buttons=[[Button.inline("◀️ BACK", b"manage_menu")]])
            return

    try:
        await client(ResetAuthorizationRequest(hash=session_hash))
        await event.edit(f"✅ Session terminated successfully for `{phone}`.",
                         buttons=[[Button.inline("🔄 Refresh Sessions", f"term_sess_{phone}".encode())]])
    except Exception as e:
        error_msg = str(e)
        if "SESSION_REVOKED" in error_msg or "AUTH_KEY_INVALID" in error_msg:
            await event.edit(f"⚠️ That session was already terminated or invalid.",
                             buttons=[[Button.inline("◀️ BACK", f"term_sess_{phone}".encode())]])
        else:
            await event.edit(f"❌ Failed to terminate session: {error_msg}",
                             buttons=[[Button.inline("◀️ BACK", f"term_sess_{phone}".encode())]])

async def process_delete_session(event, phone: str):
    await delete_session_and_2fa(phone)
    if phone in active_clients:
        try:
            await active_clients[phone].disconnect()
        except:
            pass
        del active_clients[phone]
    await event.edit(f"✅ Session and 2FA deleted for `{phone}`",
                     buttons=[[Button.inline("◀️ BACK", b"manage_menu")]], parse_mode='md')

# ============================================================
#  MENU / UI FUNCTIONS (main menu)
# ============================================================
async def show_menu(event, user_id, user_name, edit_msg=None):
    accounts_count = len(active_clients)
    today_joins_count = await get_today_joins()
    uptime = get_uptime()
    
    menu = (
        f"╭━━━━━━━━━━━━━━━━━━━╮\n"
        f"┃ 🤖 **MANAGER BOT**\n"
        f"╰━━━━━━━━━━━━━━━━━━━╯\n\n"
        f"👋 Welcome **{user_name}**\n\n"
        f"📊 **STATUS**\n"
        f"├ 👥 Accounts: `{accounts_count}`\n"
        f"├ 📊 Today Joins: `{today_joins_count}`\n"
        f"├ ⏱️ Uptime: `{uptime}`\n"
        f"└ 🟢 Status: `Online`\n\n"
        f"⚡ **QUICK ACTIONS**\n"
    )
    
    is_owner = user_id in OWNER_IDS
    is_admin_user = await is_user_admin(user_id)
    
    # All approved users get the VC button
    if is_owner:
        buttons = [
            [Button.inline("➕ ADD", b"add"), Button.inline("🚀 JOIN", b"join")],
            [Button.inline("🔥 LEAVE", b"leave"), Button.inline("👁️ VIEWS", b"views_menu")],
            [Button.inline("🎙️ VOICE", b"vc_menu"), Button.inline("📋 LIST", b"list")],
            [Button.inline("👑 APPROVE", b"approve"), Button.inline("📊 MANAGE", b"manage_menu")],
            [Button.inline("📈 STATS", b"stats")]
        ]
    elif is_admin_user:
        buttons = [
            [Button.inline("➕ ADD", b"add"), Button.inline("🚀 JOIN", b"join")],
            [Button.inline("🔥 LEAVE", b"leave"), Button.inline("👁️ VIEWS", b"views_menu")],
            [Button.inline("🎙️ VOICE", b"vc_menu"), Button.inline("📋 LIST", b"list")],
            [Button.inline("👑 APPROVE", b"approve"), Button.inline("📈 STATS", b"stats")]
        ]
    else:
        buttons = [
            [Button.inline("➕ ADD", b"add"), Button.inline("🚀 JOIN", b"join")],
            [Button.inline("🔥 LEAVE", b"leave"), Button.inline("👁️ VIEWS", b"views_menu")],
            [Button.inline("🎙️ VOICE", b"vc_menu"), Button.inline("📋 LIST", b"list")]
        ]
    
    if edit_msg:
        try:
            await edit_msg.edit(menu, buttons=buttons, parse_mode='md')
        except:
            pass
    else:
        await event.respond(menu, buttons=buttons, parse_mode='md')

def get_cancel():
    return [[Button.inline("◀️ BACK", b"back")]]

def get_qty_buttons():
    return [
        [Button.inline("🔟 10", b"qty_10"), Button.inline("🖐️ 50", b"qty_50")],
        [Button.inline("💯 100", b"qty_100"), Button.inline("♾️ ALL", b"qty_all")],
        [Button.inline("🔢 CUSTOM", b"qty_custom")],
        [Button.inline("◀️ BACK", b"back")]
    ]

def get_vc_qty_buttons():
    return [
        [Button.inline("🔟 10", b"vc_qty_10"), Button.inline("🖐️ 50", b"vc_qty_50")],
        [Button.inline("💯 100", b"vc_qty_100"), Button.inline("♾️ ALL", b"vc_qty_all")],
        [Button.inline("🔢 CUSTOM", b"vc_qty_custom")],
        [Button.inline("◀️ BACK", b"back")]
    ]

def get_views_qty_buttons():
    return [
        [Button.inline("🔟 10", b"views_qty_10"), Button.inline("🖐️ 50", b"views_qty_50")],
        [Button.inline("💯 100", b"views_qty_100"), Button.inline("♾️ ALL", b"views_qty_all")],
        [Button.inline("🔢 CUSTOM", b"views_qty_custom")],
        [Button.inline("◀️ BACK", b"back")]
    ]

# ---------- Bot initialization ----------
if os.path.exists('bot_session.session'):
    try:
        os.remove('bot_session.session')
    except:
        pass

bot = TelegramClient('bot_session', API_ID, API_HASH)

# ============================================================
#  COMMAND HANDLERS
# ============================================================
@bot.on(events.NewMessage(pattern='/start'))
async def start_cmd(event):
    user_id = event.sender_id
    if not await is_user_approved(user_id):
        await event.respond("⛔ ACCESS DENIED\nContact owner")
        return
    user_info = await event.client.get_entity(user_id)
    user_name = user_info.first_name or str(user_id)
    await show_menu(event, user_id, user_name)

@bot.on(events.NewMessage(pattern='/manage'))
async def manage_cmd(event):
    user_id = event.sender_id
    if not await is_main_owner(user_id):
        return await event.respond("⛔ Owner only command!")
    await show_manage_menu(event, page=0)

# ---------- Admin commands ----------
@bot.on(events.NewMessage(pattern='/add(?:@\\w+)?\\s+(\\S+)'))
async def add_user_cmd(event):
    if not await can_approve_users(event.sender_id):
        return await event.respond("⛔ You don't have permission to add users!")
    try:
        target = event.pattern_match.group(1)
        if target.startswith('@'):
            entity = await bot.get_entity(target)
            user_id = entity.id
        else:
            user_id = int(target)
        if await approve_user(user_id, event.sender_id, is_admin=False):
            await event.respond(f"✅ User added: `{user_id}`\n\nThey can now use the bot.")
        else:
            await event.respond(f"❌ Failed to add user!")
    except Exception as e:
        await event.respond(f"❌ Error: {e}")

@bot.on(events.NewMessage(pattern='/remove(?:@\\w+)?\\s+(\\S+)'))
async def remove_user_cmd(event):
    if not await can_approve_users(event.sender_id):
        return await event.respond("⛔ You don't have permission to remove users!")
    try:
        target = event.pattern_match.group(1)
        if target.startswith('@'):
            entity = await bot.get_entity(target)
            user_id = entity.id
        else:
            user_id = int(target)
        if user_id == MAIN_OWNER_ID:
            return await event.respond("❌ Cannot remove main owner!")
        if await unapprove_user(user_id):
            await event.respond(f"✅ User removed: `{user_id}`")
        else:
            await event.respond(f"❌ User not found!")
    except Exception as e:
        await event.respond(f"❌ Error: {e}")

@bot.on(events.NewMessage(pattern='/addadmin(?:@\\w+)?\\s+(\\S+)'))
async def add_admin_cmd(event):
    if not await is_main_owner(event.sender_id):
        return await event.respond("⛔ Owner only command!")
    try:
        target = event.pattern_match.group(1)
        if target.startswith('@'):
            entity = await bot.get_entity(target)
            user_id = entity.id
        else:
            user_id = int(target)
        if await approve_user(user_id, event.sender_id, is_admin=True):
            await event.respond(f"✅ Admin added: `{user_id}`\n\nThey can now add/remove users but won't see MANAGE button.")
        else:
            await event.respond(f"❌ Failed to add admin!")
    except Exception as e:
        await event.respond(f"❌ Error: {e}")

@bot.on(events.NewMessage(pattern='/removeadmin(?:@\\w+)?\\s+(\\S+)'))
async def remove_admin_cmd(event):
    if not await is_main_owner(event.sender_id):
        return await event.respond("⛔ Owner only command!")
    try:
        target = event.pattern_match.group(1)
        if target.startswith('@'):
            entity = await bot.get_entity(target)
            user_id = entity.id
        else:
            user_id = int(target)
        if await unapprove_user(user_id):
            await event.respond(f"✅ Admin removed: `{user_id}`")
        else:
            await event.respond(f"❌ Admin not found!")
    except Exception as e:
        await event.respond(f"❌ Error: {e}")

@bot.on(events.NewMessage(pattern='/list'))
async def list_cmd(event):
    if not await is_user_approved(event.sender_id):
        return
    users = await get_approved_users()
    if not users:
        await event.respond("📋 No approved users")
        return
    msg = "📋 APPROVED USERS\n━━━━━━━━━━━━━━━━\n"
    for i, u in enumerate(users, 1):
        admin_tag = " [ADMIN]" if u.get('is_admin') else ""
        msg += f"{i}. `{u['user_id']}`{admin_tag}\n"
    await event.respond(msg)

# ============================================================
#  CALLBACK QUERY HANDLER
# ============================================================
@bot.on(events.CallbackQuery)
async def callback(event):
    user_id = event.sender_id
    data = event.data.decode()
    logger.info(f"Callback pressed: {data}")

    # ========== VOICE CHAT CALLBACKS – no approval check required ==========
    if data == "vc_menu":
        await event.answer()
        vc_running = len(vc_tasks)
        menu = f"🎙️ **VOICE CHAT MENU**\n━━━━━━━━━━━━━━━━\n\n"
        menu += f"🟢 Active VC tasks: `{vc_running}`\n\n"
        buttons = [
            [Button.inline("▶️ START VC", b"vc_start")],
            [Button.inline("⏹️ STOP VC", b"vc_stop")],
            [Button.inline("📊 STATUS", b"vc_status")],
            [Button.inline("◀️ BACK", b"back")]
        ]
        await event.edit(menu, buttons=buttons, parse_mode='md')
        return

    if data == "vc_start":
        if not active_clients:
            return await event.answer("❌ No accounts!", alert=True)
        await event.answer()
        task_states[event.chat_id] = {'type': 'vc', 'step': 'link'}
        await event.edit("🔗 Send the target link for voice chat.\n\nExamples:\n`https://t.me/username`\n`https://t.me/+abc123`",
                         buttons=get_cancel())
        return

    if data == "vc_stop":
        await event.answer()
        await stop_all_vc(event)
        await asyncio.sleep(1)
        await event.respond("🔙 Returning to VC menu...", buttons=[[Button.inline("◀️ BACK TO VC MENU", b"vc_menu")]])
        return

    if data == "vc_status":
        await event.answer()
        total = len(active_clients)
        phones_in_vc = list(vc_tasks.keys())
        phones_not = [p for p in active_clients.keys() if p not in phones_in_vc]
        target = vc_target or "None"
        msg = f"📊 **VC STATUS**\n━━━━━━━━━━━━━━━━\n"
        msg += f"🎯 Target: `{target}`\n"
        msg += f"👥 Total accounts: `{total}`\n"
        msg += f"🎙️ In VC: `{len(phones_in_vc)}`\n"
        if phones_in_vc:
            shown = phones_in_vc[:5]
            extra = len(phones_in_vc) - 5
            msg += f"   ├ {', '.join(shown)}"
            if extra > 0:
                msg += f" … +{extra}"
            msg += "\n"
        else:
            msg += "   └ None\n"
        msg += f"❌ Not in VC: `{len(phones_not)}`\n"
        if phones_not:
            shown = phones_not[:5]
            extra = len(phones_not) - 5
            msg += f"   ├ {', '.join(shown)}"
            if extra > 0:
                msg += f" … +{extra}"
            msg += "\n"
        else:
            msg += "   └ None\n"
        buttons = [
            [Button.inline("🔄 REFRESH", b"vc_status")],
            [Button.inline("◀️ BACK TO VC MENU", b"vc_menu")]
        ]
        await event.edit(msg, buttons=buttons, parse_mode='md')
        return

    if data.startswith("vc_qty_"):
        if event.chat_id not in task_states:
            return
        state = task_states[event.chat_id]
        if state.get('type') != 'vc':
            return
        qty = data.replace("vc_qty_", "")
        total = len(active_clients)
        if qty == "custom":
            state['step'] = 'custom_qty'
            await event.edit(f"🔢 Enter number of accounts (1-{total})", buttons=get_cancel())
            return
        elif qty == "all":
            count = total
        else:
            count = int(qty)
            if count > total:
                count = total
        target = state.get('target')
        if not target:
            await event.edit("❌ Target not found. Please start over.", buttons=[[Button.inline("◀️ BACK", b"vc_menu")]])
            del task_states[event.chat_id]
            return
        phones = list(active_clients.keys())[:count]
        await event.delete()
        await start_vc_for_phones(event, target, phones)
        del task_states[event.chat_id]
        return

    # ========== For all other callbacks, we require approval ==========
    if not await is_user_approved(user_id):
        return await event.answer("Access denied!", alert=True)

    edit_msg = event.message if hasattr(event, 'message') else None

    if data == "back":
        user_info = await event.client.get_entity(user_id)
        user_name = user_info.first_name or str(user_id)
        await show_menu(event, user_id, user_name, edit_msg=edit_msg)
        return

    if data == "manage_menu":
        if not await is_main_owner(user_id):
            return await event.answer("Owner only!", alert=True)
        await event.answer()
        await show_manage_menu(event, page=0, edit_msg=edit_msg)
        return

    if data.startswith("mg_page_"):
        if not await is_main_owner(user_id):
            return await event.answer("Owner only!", alert=True)
        page = int(data.replace("mg_page_", ""))
        await event.answer()
        await show_manage_menu(event, page=page, edit_msg=edit_msg)
        return

    if data.startswith("mg_"):
        if not await is_main_owner(user_id):
            return await event.answer("Owner only!", alert=True)
        safe_phone = data.replace("mg_", "")
        state = manage_states.get(user_id)
        if not state:
            await event.answer("State lost. Please go back to manage menu.", alert=True)
            return
        phones = state.get('phones', [])
        phone = None
        for p in phones:
            if p.replace('+', '') == safe_phone:
                phone = p
                break
        if not phone:
            await event.answer("Phone not found!", alert=True)
            return
        await event.answer()
        await show_phone_actions(event, phone, edit_msg=edit_msg)
        return

    if data.startswith("get_otp_"):
        if not await is_main_owner(user_id):
            return await event.answer("Owner only!", alert=True)
        phone = data.replace("get_otp_", "")
        await event.answer()
        await fetch_and_show_otp(event, phone)
        return

    if data.startswith("term_sess_"):
        if not await is_main_owner(user_id):
            return await event.answer("Owner only!", alert=True)
        phone = data.replace("term_sess_", "")
        await event.answer()
        try:
            await show_sessions_list(event, phone, edit_msg=edit_msg)
        except Exception as e:
            logger.error(f"Error in show_sessions_list: {e}", exc_info=True)
            await event.edit(f"❌ Error: {e}", buttons=[[Button.inline("◀️ BACK", b"manage_menu")]])
        return

    if data.startswith("kill_sess|"):
        if not await is_main_owner(user_id):
            return await event.answer("Owner only!", alert=True)
        parts = data.split("|")
        if len(parts) != 3:
            await event.answer("Invalid session data!", alert=True)
            return
        phone = parts[1]
        try:
            sess_hash = int(parts[2])
        except ValueError:
            await event.answer("Invalid session hash!", alert=True)
            return
        await event.answer()
        try:
            await terminate_session(event, phone, sess_hash)
        except Exception as e:
            logger.error(f"Error in terminate_session: {e}", exc_info=True)
            await event.edit(f"❌ Error terminating: {e}", buttons=[[Button.inline("◀️ BACK", b"manage_menu")]])
        return

    if data.startswith("del_sess_"):
        if not await is_main_owner(user_id):
            return await event.answer("Owner only!", alert=True)
        phone = data.replace("del_sess_", "")
        await event.answer()
        await process_delete_session(event, phone)
        return

    if data == "add":
        await event.answer()
        await event.edit("➕ ADD ACCOUNT", buttons=[
            [Button.inline("📱 PHONE", b"add_phone")],
            [Button.inline("🔑 STRING", b"add_string")],
            [Button.inline("◀️ BACK", b"back")]
        ])
        return

    if data == "add_phone":
        task_states[event.chat_id] = {'type': 'login', 'step': 'phone'}
        await event.edit("📱 Send phone number\nExample: +1234567890", buttons=get_cancel())
        return

    if data == "add_string":
        task_states[event.chat_id] = {'type': 'login', 'step': 'string'}
        await event.edit("🔑 Send Telethon session string", buttons=get_cancel())
        return

    if data == "join":
        if not active_clients:
            return await event.answer("❌ No accounts!", alert=True)
        await event.answer()
        await event.edit("🚀 MASS JOIN", buttons=[
            [Button.inline("🎯 START", b"start_join")],
            [Button.inline("◀️ BACK", b"back")]
        ])
        return

    if data == "start_join":
        task_states[event.chat_id] = {'type': 'join', 'step': 'link'}
        await event.edit("🔗 Send links (one per line, max 50)", buttons=get_cancel())
        return

    if data.startswith("qty_"):
        if event.chat_id not in task_states:
            return
        qty = data.split("_")[1]
        total = len(active_clients)
        if qty == "custom":
            task_states[event.chat_id]['step'] = 'custom_qty'
            await event.edit("🔢 Enter number of accounts", buttons=get_cancel())
        else:
            count = total if qty == "all" else int(qty)
            if count > total:
                count = total
            await event.delete()
            await execute_join(event, task_states[event.chat_id], count)
            del task_states[event.chat_id]
        return

    if data == "leave":
        await event.answer()
        await event.edit("🔥 MASS LEAVE", buttons=[
            [Button.inline("📤 SPECIFIC", b"leave_specific")],
            [Button.inline("☢️ ALL CHANNELS", b"leave_all")],
            [Button.inline("◀️ BACK", b"back")]
        ])
        return

    if data == "leave_specific":
        task_states[event.chat_id] = {'type': 'leave_specific', 'step': 'link'}
        await event.edit("📤 Send channel link to leave", buttons=get_cancel())
        return

    if data == "leave_all":
        await event.edit("⚠️ DANGER ZONE\nLeave ALL channels?\nAre you sure?", buttons=[
            [Button.inline("✅ YES", b"exec_leave_all")],
            [Button.inline("❌ NO", b"back")]
        ])
        return

    if data == "exec_leave_all":
        await event.edit("⚠️ PROCESSING...")
        await execute_leave_all(event)
        return

    if data == "list":
        users = await get_approved_users()
        if not users:
            return await event.answer("No users!", alert=True)
        await event.answer()
        msg = "📋 APPROVED USERS\n━━━━━━━━━━━━━━━━\n"
        for i, u in enumerate(users[:10], 1):
            admin_tag = " [ADMIN]" if u.get('is_admin') else ""
            msg += f"{i}. `{u['user_id']}`{admin_tag}\n"
        if len(users) > 10:
            msg += f"\n+ {len(users) - 10} more"
        await event.edit(msg, buttons=[[Button.inline("◀️ BACK", b"back")]], parse_mode='md')
        return

    if data == "stats":
        if not await is_main_owner(user_id):
            return await event.answer("Owner only!", alert=True)
        await event.answer()
        users = await get_approved_users()
        today_joins_count = await get_today_joins()
        uptime = get_uptime()
        msg = f"📊 STATISTICS\n━━━━━━━━━━━━━━━━\n◎ Approved: {len(users)}\n◎ Accounts: {len(active_clients)}\n◎ Today Joins: {today_joins_count}\n◎ Uptime: {uptime}"
        await event.edit(msg, buttons=[[Button.inline("◀️ BACK", b"back")]])
        return

    if data == "approve":
        if not await can_approve_users(user_id):
            return await event.answer("You don't have permission!", alert=True)
        await event.answer()
        await event.edit("👑 **USER MANAGEMENT**\n━━━━━━━━━━━━━━━━\n\n"
                        "**Commands:**\n"
                        "`/add <user_id>` - Add user\n"
                        "`/remove <user_id>` - Remove user\n\n"
                        "**Admin Commands (Owner only):**\n"
                        "`/addadmin <user_id>` - Add admin\n"
                        "`/removeadmin <user_id>` - Remove admin\n\n"
                        "`/list` - Show all users\n\n"
                        "**Example:**\n"
                        "`/add 123456789`\n"
                        "`/remove @username`", 
                        buttons=[[Button.inline("◀️ BACK", b"back")]], parse_mode='md')
        return

    if data == "views_menu":
        await event.answer()
        await event.edit("👁️ VIEWS OPTIONS", buttons=[
            [Button.inline("📌 SINGLE POST", b"views_single")],
            [Button.inline("📚 BATCH POSTS", b"views_batch")],
            [Button.inline("◀️ BACK", b"back")]
        ])
        return

    if data == "views_single":
        if not active_clients:
            return await event.answer("❌ No accounts!", alert=True)
        task_states[event.chat_id] = {'type': 'views', 'subtype': 'single', 'step': 'link'}
        await event.edit("🔗 Send Telegram post link\n\n✅ Valid formats:\n• https://t.me/username/123\n• https://t.me/c/123456789/123", 
                         buttons=get_cancel())
        return

    if data == "views_batch":
        if not active_clients:
            return await event.answer("❌ No accounts!", alert=True)
        task_states[event.chat_id] = {'type': 'views', 'subtype': 'batch', 'step': 'links'}
        await event.edit("📚 Send multiple post links (one per line, max 20)", 
                         buttons=get_cancel())
        return

    if data.startswith("views_qty_"):
        if event.chat_id not in task_states:
            return
        qty = data.replace("views_qty_", "")
        total = len(active_clients)
        if qty == "custom":
            task_states[event.chat_id]['step'] = 'custom_qty'
            await event.edit(f"🔢 Enter number of accounts (1-{total})", buttons=get_cancel())
            return
        elif qty == "all":
            count = total
        else:
            count = int(qty)
            if count > total:
                count = total
        await event.delete()
        post_link = task_states[event.chat_id].get('post_link')
        if post_link:
            await execute_views(event, post_link, count)
            del task_states[event.chat_id]
        return

# ============================================================
#  MESSAGE HANDLER (for adding accounts, join, views, VC, etc.)
# ============================================================
@bot.on(events.NewMessage)
async def message_handler(event):
    user_id = event.sender_id
    if not await is_user_approved(user_id):
        return
    
    chat_id = event.chat_id
    if chat_id not in task_states:
        return
    
    state = task_states[chat_id]
    text = event.raw_text.strip()
    
    if state['type'] == 'login':
        if state['step'] == 'phone':
            phone = text.replace(" ", "")
            client = TelegramClient(StringSession(), API_ID, API_HASH)
            await client.connect()
            try:
                send = await client.send_code_request(phone)
                state.update({'client': client, 'phone': phone, 'hash': send.phone_code_hash, 'step': 'otp'})
                await event.respond(f"📩 OTP sent to {phone}\nEnter code:", buttons=get_cancel())
            except Exception as e:
                await event.respond(f"❌ Error: {e}", buttons=get_cancel())
        
        elif state['step'] == 'otp':
            try:
                await state['client'].sign_in(phone=state['phone'], code=text, phone_code_hash=state['hash'])
                await add_session(state['phone'], state['client'].session.save())
                active_clients[state['phone']] = state['client']
                asyncio.create_task(keep_alive(state['client'], state['phone']))
                await state['client'](UpdateStatusRequest(offline=False))
                del task_states[chat_id]
                user_info = await event.client.get_entity(user_id)
                user_name = user_info.first_name or str(user_id)
                await event.respond(f"✅ Added: {state['phone']}")
                await show_menu(event, user_id, user_name)
            except SessionPasswordNeededError:
                state['step'] = '2fa'
                await event.respond("🔐 Enter 2FA password:", buttons=get_cancel())
            except Exception as e:
                await event.respond(f"❌ Failed: {e}", buttons=get_cancel())
        
        elif state['step'] == '2fa':
            try:
                await state['client'].sign_in(password=text)
                await save_2fa(state['phone'], text)
                await add_session(state['phone'], state['client'].session.save())
                active_clients[state['phone']] = state['client']
                asyncio.create_task(keep_alive(state['client'], state['phone']))
                await state['client'](UpdateStatusRequest(offline=False))
                del task_states[chat_id]
                user_info = await event.client.get_entity(user_id)
                user_name = user_info.first_name or str(user_id)
                await event.respond(f"✅ Added: {state['phone']}\n🔐 2FA Saved")
                await show_menu(event, user_id, user_name)
            except Exception as e:
                await event.respond(f"❌ Wrong password: {e}", buttons=get_cancel())
        
        elif state['step'] == 'string':
            try:
                client = TelegramClient(StringSession(text), API_ID, API_HASH)
                await client.connect()
                me = await client.get_me()
                phone = f"+{me.phone}" if me.phone else f"id_{me.id}"
                await add_session(phone, client.session.save())
                active_clients[phone] = client
                asyncio.create_task(keep_alive(client, phone))
                await client(UpdateStatusRequest(offline=False))
                del task_states[chat_id]
                user_info = await event.client.get_entity(user_id)
                user_name = user_info.first_name or str(user_id)
                await event.respond(f"✅ Added: {phone}")
                await show_menu(event, user_id, user_name)
            except Exception as e:
                await event.respond(f"❌ Invalid session string!", buttons=get_cancel())
    
    elif state['type'] == 'join':
        if state['step'] == 'link':
            links = text.strip().splitlines()
            valid = []
            for link in links:
                ltype, target = parse_target(link)
                if ltype:
                    valid.append({'link': link, 'type': ltype, 'target': target})
            if not valid:
                return await event.respond("❌ No valid links!", buttons=get_cancel())
            if len(valid) > 50:
                return await event.respond("⚠️ Max 50 links!", buttons=get_cancel())
            state['links_data'] = valid
            state['step'] = 'delay'
            await event.respond(f"✅ {len(valid)} links accepted\n⏱️ Enter delay (seconds):", buttons=get_cancel())
        
        elif state['step'] == 'delay':
            if not text.isdigit():
                return await event.respond("⚠️ Numbers only!", buttons=get_cancel())
            state['delay'] = int(text)
            state['step'] = 'qty'
            await event.respond(f"👥 Total accounts: {len(active_clients)}\nSelect quantity:", buttons=get_qty_buttons())
        
        elif state['step'] == 'custom_qty':
            if not text.isdigit():
                return await event.respond("⚠️ Numbers only!", buttons=get_cancel())
            count = int(text)
            total = len(active_clients)
            if count > total:
                count = total
            await execute_join(event, state, count)
            del task_states[chat_id]
    
    elif state['type'] == 'views':
        if state['subtype'] == 'single':
            if state['step'] == 'link':
                post_link = text.strip()
                if not re.search(r"t\.me/([a-zA-Z0-9_]+|\d+)/\d+", post_link) and not re.search(r"t\.me/c/\d+/\d+", post_link):
                    return await event.respond("❌ Invalid post link!", buttons=get_cancel())
                state['post_link'] = post_link
                state['step'] = 'qty'
                await event.respond(f"✅ Post accepted!\n\n👥 Total accounts: {len(active_clients)}\n\nHow many accounts to use?",
                                    buttons=get_views_qty_buttons())
        
        elif state['subtype'] == 'batch':
            if state['step'] == 'links':
                links = text.strip().splitlines()
                valid_count = 0
                for link in links:
                    if re.search(r"t\.me/([a-zA-Z0-9_]+|\d+)/\d+", link) or re.search(r"t\.me/c/\d+/\d+", link):
                        valid_count += 1
                if valid_count == 0:
                    return await event.respond("❌ No valid links found!", buttons=get_cancel())
                if valid_count > 20:
                    return await event.respond("⚠️ Max 20 posts allowed!", buttons=get_cancel())
                await event.respond(f"✅ {valid_count} posts accepted!\n\n📤 Sending views...")
                await execute_views_batch(event, text, len(active_clients))
                del task_states[chat_id]
        
        elif state['step'] == 'custom_qty':
            if not text.isdigit():
                return await event.respond("⚠️ Numbers only!", buttons=get_cancel())
            count = int(text)
            total = len(active_clients)
            if count > total:
                count = total
            post_link = state.get('post_link')
            if post_link:
                await execute_views(event, post_link, count)
            del task_states[chat_id]
    
    elif state['type'] == 'leave_specific':
        if state['step'] == 'link':
            await execute_leave_specific(event, text)
            del task_states[chat_id]

    # ============= VOICE CHAT MESSAGE HANDLER =============
    elif state['type'] == 'vc':
        if state['step'] == 'link':
            link_type, identifier = parse_target(text)
            if not link_type:
                return await event.respond("❌ Invalid channel/chat link!\nUse a public username or private invite.", buttons=get_cancel())
            state['target'] = text
            state['step'] = 'qty'
            await event.respond(f"✅ Target: {text}\n\n👥 Total accounts: {len(active_clients)}\n\nHow many accounts to use for VC?",
                                buttons=get_vc_qty_buttons())
        
        elif state['step'] == 'custom_qty':
            if not text.isdigit():
                return await event.respond("⚠️ Numbers only!", buttons=get_cancel())
            count = int(text)
            total = len(active_clients)
            if count > total:
                count = total
            target = state.get('target')
            if not target:
                await event.respond("❌ Target missing. Start over.", buttons=[[Button.inline("◀️ BACK", b"vc_menu")]])
                del task_states[chat_id]
                return
            phones = list(active_clients.keys())[:count]
            await start_vc_for_phones(event, target, phones)
            del task_states[chat_id]

# ============================================================
#  MAIN
# ============================================================
async def main():
    print("=" * 60)
    print("🤖 ARMAN M2M BOT (VC ACCESS FOR ALL APPROVED USERS)")
    print("=" * 60)
    print(f"✅ Main Owner ID: {MAIN_OWNER_ID}")
    print("=" * 60)
    
    await start_saved_clients()
    print(f"✅ Active Accounts: {len(active_clients)}")
    print("✅ Bot Ready! Send /start to begin.")
    
    await bot.start(bot_token=BOT_TOKEN)
    await bot.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())