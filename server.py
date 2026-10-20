import asyncio
import json
import websockets
import http.server
import socketserver
import threading
import os
import time

HOST = "0.0.0.0"
HTTP_PORT = 8000
WS_PORT = 8765

clients = {}
game_state = {"p1": None, "p2": None}
ready_state = {"p1": False, "p2": False}

game_running = False
current_round = 1
p1_wins = 0
p2_wins = 0
round_time = 120
last_tick = 0
round_over = False

async def game_loop():
    global round_time, last_tick, game_running, p1_wins, p2_wins, current_round, round_over
    
    while True:
        if game_running and game_state["p1"] and game_state["p2"]:
            now = time.time()
            if now - last_tick >= 1.0:
                round_time -= 1
                last_tick = now
                
                # Отправляем время обоим
                time_msg = json.dumps({"type": "time_sync", "time": round_time, "p1Wins": p1_wins, "p2Wins": p2_wins, "round": current_round})
                try:
                    await game_state["p1"].send(time_msg)
                    await game_state["p2"].send(time_msg)
                except:
                    pass
                
                if round_time <= 0 and not round_over:
                    round_over = True
                    game_running = False
                    # Ничья по времени (упрощенно)
                    p1_wins += 1 
                    p2_wins += 1
                    end_msg = json.dumps({"type": "round_end", "winner": "DRAW"})
                    try:
                        await game_state["p1"].send(end_msg)
                        await game_state["p2"].send(end_msg)
                    except: pass
        await asyncio.sleep(0.1)

async def ws_handler(websocket):
    global game_running, current_round, p1_wins, p2_wins, round_time, last_tick, round_over
    
    player_id = None
    if game_state["p1"] is None:
        player_id = "p1"
        game_state["p1"] = websocket
    elif game_state["p2"] is None:
        player_id = "p2"
        game_state["p2"] = websocket
    else:
        await websocket.close()
        return

    clients[websocket] = player_id
    ready_state[player_id] = False
    await websocket.send(json.dumps({"type": "welcome", "player": player_id}))

    if game_state["p1"] and game_state["p2"]:
        await game_state["p1"].send(json.dumps({"type": "both_connected"}))
        await game_state["p2"].send(json.dumps({"type": "both_connected"}))

    try:
        async for message in websocket:
            data = json.loads(message)
            
            if data.get("type") == "ready":
                ready_state[player_id] = True
                if ready_state["p1"] and ready_state["p2"]:
                    # Сбрасываем таймеры при старте
                    current_round = 1
                    p1_wins = 0
                    p2_wins = 0
                    round_time = 120
                    last_tick = time.time()
                    round_over = False
                    game_running = True
                    
                    start_msg = json.dumps({"type": "start", "round": current_round, "p1Wins": p1_wins, "p2Wins": p2_wins})
                    await game_state["p1"].send(start_msg)
                    await game_state["p2"].send(start_msg)
                else:
                    other = game_state["p2"] if player_id == "p1" else game_state["p1"]
                    if other: await other.send(json.dumps({"type": "opponent_ready"}))
            
            elif data.get("type") == "round_win":
                # Если один из игроков убил другого
                if not round_over:
                    round_over = True
                    game_running = False
                    if data["winner"] == "P1": p1_wins += 1
                    else: p2_wins += 1
                    
                    end_msg = json.dumps({"type": "round_end", "winner": data["winner"], "p1Wins": p1_wins, "p2Wins": p2_wins})
                    await game_state["p1"].send(end_msg)
                    await game_state["p2"].send(end_msg)
                    
                    # Ждем 3 секунды и запускаем новый раунд
                    await asyncio.sleep(3)
                    current_round += 1
                    round_time = 120
                    last_tick = time.time()
                    round_over = False
                    
                    if p1_wins >= 3 or p2_wins >= 3:
                        final_msg = json.dumps({"type": "match_end", "p1Wins": p1_wins, "p2Wins": p2_wins})
                        await game_state["p1"].send(final_msg)
                        await game_state["p2"].send(final_msg)
                        game_running = False
                    else:
                        start_msg = json.dumps({"type": "start", "round": current_round, "p1Wins": p1_wins, "p2Wins": p2_wins})
                        await game_state["p1"].send(start_msg)
                        await game_state["p2"].send(start_msg)
                        game_running = True

            else:
                other = game_state["p2"] if player_id == "p1" else game_state["p1"]
                if other:
                    data["from"] = player_id
                    await other.send(json.dumps(data))
                    
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clients.pop(websocket, None)
        if game_state[player_id] == websocket: game_state[player_id] = None
        ready_state[player_id] = False
        other = game_state["p1"] or game_state["p2"]
        if other:
            try: await other.send(json.dumps({"type": "opponent_left"}))
            except: pass

async def main():
    print(f"[WS] Сервер: ws://{HOST}:{WS_PORT}")
    asyncio.create_task(game_loop())
    async with websockets.serve(ws_handler, HOST, WS_PORT):
        await asyncio.Future()

def start_http():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    class Handler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, format, *args): pass
    with socketserver.TCPServer((HOST, HTTP_PORT), Handler) as httpd:
        httpd.serve_forever()

if __name__ == "__main__":
    threading.Thread(target=start_http, daemon=True).start()
    asyncio.run(main())