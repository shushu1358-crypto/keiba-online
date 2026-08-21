const http = require("http");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 10000);
const rooms = new Map();

function makeRoomCode() {
  let code;
  do {
    code = "KB" + crypto.randomBytes(3).toString("hex").toUpperCase();
  } while (rooms.has(code));
  return code;
}

function wsFrame(text) {
  const body = Buffer.from(text);
  let header;

  if (body.length < 126) {
    header = Buffer.alloc(2);
    header[1] = body.length;
  } else if (body.length < 65536) {
    header = Buffer.alloc(4);
    header[1] = 126;
    header.writeUInt16BE(body.length, 2);
  } else {
    header = Buffer.alloc(10);
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(body.length), 2);
  }

  header[0] = 0x81;
  return Buffer.concat([header, body]);
}

function send(ws, data) {
  if (!ws.destroyed) {
    ws.write(wsFrame(JSON.stringify(data)));
  }
}

function publicPlayers(room) {
  return room.players.map(p => ({
    id: p.id,
    name: p.name,
    host: p.host,
    ready: p.ready
  }));
}

function broadcast(room, data) {
  const frame = wsFrame(JSON.stringify(data));
  for (const player of room.players) {
    if (!player.ws.destroyed) player.ws.write(frame);
  }
}

function broadcastRoomState(room) {
  broadcast(room, {
    type: "room_state",
    code: room.code,
    name: room.name,
    size: room.size,
    format: room.format,
    prize: room.prize || 0,
    players: publicPlayers(room)
  });
}

function removePlayer(ws) {
  for (const [code, room] of rooms) {
    const index = room.players.findIndex(p => p.ws === ws);
    if (index === -1) continue;

    const wasHost = room.players[index].host;
    room.players.splice(index, 1);

    if (room.players.length === 0) {
      rooms.delete(code);
      return;
    }

    if (wasHost) {
      room.players[0].host = true;
    }

    broadcastRoomState(room);
    return;
  }
}

function parseWebSocketFrames(ws) {
  let buffer = Buffer.alloc(0);

  return data => {
    buffer = Buffer.concat([buffer, data]);

    while (buffer.length >= 2) {
      const first = buffer[0];
      const second = buffer[1];
      const opcode = first & 0x0f;
      const masked = !!(second & 0x80);
      let length = second & 0x7f;
      let offset = 2;

      if (length === 126) {
        if (buffer.length < 4) return;
        length = buffer.readUInt16BE(2);
        offset = 4;
      } else if (length === 127) {
        if (buffer.length < 10) return;
        length = Number(buffer.readBigUInt64BE(2));
        offset = 10;
      }

      const total = offset + (masked ? 4 : 0) + length;
      if (buffer.length < total) return;

      let mask;
      if (masked) {
        mask = buffer.subarray(offset, offset + 4);
        offset += 4;
      }

      const payload = Buffer.from(buffer.subarray(offset, offset + length));

      if (mask) {
        for (let i = 0; i < payload.length; i++) {
          payload[i] ^= mask[i % 4];
        }
      }

      buffer = buffer.subarray(total);

      if (opcode === 0x8) {
        ws.end();
        return;
      }

      if (opcode === 0x1) {
        try {
          handleMessage(ws, JSON.parse(payload.toString("utf8")));
        } catch {
          send(ws, {
            type: "error",
            message: "不正な通信データを受信しました。"
          });
        }
      }
    }
  };
}

function handleMessage(ws, message) {
  if (message.type === "create_room") {
    const code = makeRoomCode();

    const player = {
      ws,
      id: crypto.randomUUID(),
      name: String(message.playerName || "プレイヤー").slice(0, 12),
      host: true,
      ready: false,
      horse: null
    };

    const room = {
      code,
      name: String(message.name || "ミント杯").slice(0, 24),
      size: Math.min(Math.max(Number(message.size) || 8, 2), 32),
      format: String(message.format || "トーナメント"),
      prize: Math.max(0, Math.min(1000000000000, Math.floor(Number(message.prize) || 0))),
      raceActive: false,
      raceNo: 0,
      players: [player]
    };

    rooms.set(code, room);
    ws.roomCode = code;

    send(ws, {
      type: "room_created",
      code,
      playerId: player.id,
      host: true,
      size: room.size,
      prize: room.prize || 0,
      players: publicPlayers(room)
    });

    return;
  }

  if (message.type === "join_room") {
    const code = String(message.code || "").trim().toUpperCase();
    const room = rooms.get(code);

    if (!room) {
      send(ws, {
        type: "error",
        message: "大会が見つかりません。"
      });
      return;
    }

    if (room.players.length >= room.size) {
      send(ws, {
        type: "error",
        message: "大会の定員に達しています。"
      });
      return;
    }

    const player = {
      ws,
      id: crypto.randomUUID(),
      name: String(message.playerName || "プレイヤー").slice(0, 12),
      host: false,
      ready: false,
      horse: null
    };

    room.players.push(player);
    ws.roomCode = code;

    send(ws, {
      type: "room_joined",
      code,
      playerId: player.id,
      host: false,
      size: room.size,
      prize: room.prize || 0,
      players: publicPlayers(room)
    });

    broadcastRoomState(room);
    return;
  }

  if (message.type === "leave_room") {
    const room = rooms.get(ws.roomCode);
    if (!room) return;
    const index = room.players.findIndex(p => p.ws === ws);
    if (index < 0) return;
    const wasHost = room.players[index].host;
    room.players.splice(index, 1);
    ws.roomCode = null;
    if (!room.players.length) {
      rooms.delete(room.code);
      send(ws, { type: "left_room" });
      return;
    }
    if (wasHost) room.players[0].host = true;
    send(ws, { type: "left_room" });
    broadcastRoomState(room);
    return;
  }

  if (message.type === "ready") {
    const room = rooms.get(ws.roomCode);
    if (!room) return;

    const player = room.players.find(p => p.ws === ws);
    if (!player) return;

    player.ready = !!message.ready;
    player.horse = message.horse || null;

    broadcastRoomState(room);
    return;
  }

  if (message.type === "start_race") {
    const room = rooms.get(ws.roomCode);
    if (!room) return;

    const player = room.players.find(p => p.ws === ws);
    if (!player || !player.host) {
      send(ws, { type: "error", message: "大会主催者のみレースを開始できます。" });
      return;
    }
    if (room.raceActive) {
      send(ws, { type: "error", message: "すでにオンラインレース中です。" });
      return;
    }

    const requestedPrize = Math.max(0, Math.floor(Number(message.prizePaidAmount ?? room.prize ?? 0)));
    if (requestedPrize !== Number(room.prize || 0) || message.prizePaid !== true) {
      send(ws, { type: "error", message: "大会賞金の預け入れが確認できません。主催者の賞金を用意してから開始してください。" });
      return;
    }
    if (room.prizeEscrowed) {
      send(ws, { type: "error", message: "この大会賞金はすでに預け入れ済みです。" });
      return;
    }

    const ready = room.players.filter(p => p.ready && p.horse);
    if (ready.length < 2) {
      send(ws, { type: "error", message: "2人以上が出走準備OKになってから開始してください。" });
      return;
    }

    const field = ready.map((p, i) => {
      const h = p.horse || {};
      const speed=Number(h.speed)||60, stamina=Number(h.stamina)||60;
      const power=Number(h.power)||60, guts=Number(h.guts)||60;
      return {
        number:i+1, playerId:p.id,
        name:String(h.name||p.name||"プレイヤー").slice(0,12),
        speed,stamina,power,guts,
        age:Math.max(2,Math.min(20,Math.floor(Number(h.age)||2))),
        wetAbility:Number(h.wetAbility)||70,
        style:h.style||"差し",
        bgColor:/^#[0-9a-fA-F]{6}$/.test(h.bgColor||"")?h.bgColor:"#378f50",
        ability:speed*.35+stamina*.25+power*.2+guts*.2
      };
    });

    room.raceActive=true;
    room.prizeEscrowed=true;
    room.raceNo=(room.raceNo||0)+1;
    const race={...(message.race||{}),prize:room.prize||0};

    broadcast(room,{type:"race_start",race,field});

    setTimeout(()=>{
      if(!rooms.has(room.code))return;
      const results=field.map(h=>({...h,_score:h.ability*(.78+Math.random()*.44)}))
        .sort((a,b)=>b._score-a._score)
        .map((h,i)=>{
          const r={...h,finishOrder:i+1,progress:100,finished:true};
          delete r._score; return r;
        });

      room.raceActive=false;
      room.prizeEscrowed=false;
      room.players.forEach(p=>{p.ready=false;p.horse=null;});
      broadcast(room,{type:"race_result",raceNo:room.raceNo,race,results,prizePaid:room.prize||0});
      broadcastRoomState(room);
    },5600);

    return;
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, {
      "Content-Type": "text/plain; charset=utf-8"
    });
    res.end("KEIBA ONLINE SERVER OK");
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

server.on("upgrade", (req, socket) => {
  if (String(req.headers.upgrade || "").toLowerCase() !== "websocket") {
    socket.destroy();
    return;
  }

  const key = req.headers["sec-websocket-key"];

  if (!key) {
    socket.destroy();
    return;
  }

  const accept = crypto
    .createHash("sha1")
    .update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
    .digest("base64");

  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
    "Upgrade: websocket\r\n" +
    "Connection: Upgrade\r\n" +
    `Sec-WebSocket-Accept: ${accept}\r\n` +
    "\r\n"
  );

  socket.on("data", parseWebSocketFrames(socket));
  socket.on("close", () => removePlayer(socket));
  socket.on("error", () => removePlayer(socket));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`KEIBA ONLINE SERVER listening on port ${PORT}`);
});
