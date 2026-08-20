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
    if (!player.ws.destroyed) {
      player.ws.write(frame);
    }
  }
}

function broadcastRoomState(room) {
  broadcast(room, {
    type: "room_state",
    code: room.code,
    name: room.name,
    size: room.size,
    format: room.format,
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

      const total =
        offset +
        (masked ? 4 : 0) +
        length;

      if (buffer.length < total) return;

      let mask;

      if (masked) {
        mask = buffer.subarray(offset, offset + 4);
        offset += 4;
      }

      const payload = Buffer.from(
        buffer.subarray(offset, offset + length)
      );

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
          handleMessage(
            ws,
            JSON.parse(payload.toString("utf8"))
          );
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

  // =========================
  // 大会ルーム作成
  // =========================

  if (message.type === "create_room") {

    const code = makeRoomCode();

    const player = {
      ws,
      id: crypto.randomUUID(),
      name: String(
        message.playerName || "プレイヤー"
      ).slice(0, 12),

      host: true,
      ready: false,
      horse: null
    };

    const room = {
      code,

      name: String(
        message.name || "ミント杯"
      ).slice(0, 24),

      size: Math.min(
        Math.max(
          Number(message.size) || 8,
          2
        ),
        32
      ),

      format: String(
        message.format || "トーナメント"
      ),

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
      players: publicPlayers(room)
    });

    return;
  }

  // =========================
  // 大会参加
  // =========================

  if (message.type === "join_room") {

    const code = String(
      message.code || ""
    ).trim().toUpperCase();

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

      name: String(
        message.playerName || "プレイヤー"
      ).slice(0, 12),

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
      players: publicPlayers(room)
    });

    broadcastRoomState(room);

    return;
  }

  // =========================
  // 出走準備
  // =========================

  if (message.type === "ready") {

    const room = rooms.get(ws.roomCode);

    if (!room) return;

    const player =
      room.players.find(
        p => p.ws === ws
      );

    if (!player) return;

    player.ready = !!message.ready;

    player.horse =
      message.horse || null;

    broadcastRoomState(room);

    return;
  }

  // =========================
  // オンラインレース開始
  // =========================

  if (message.type === "start_race") {

    const room = rooms.get(ws.roomCode);

    if (!room) return;

    const player =
      room.players.find(
        p => p.ws === ws
      );

    if (!player || !player.host) {

      send(ws, {
        type: "error",
        message:
          "大会主催者のみレースを開始できます。"
      });

      return;
    }

    const readyPlayers =
      room.players.filter(
        p => p.ready && p.horse
      );

    if (readyPlayers.length < 2) {

      send(ws, {
        type: "error",
        message:
          "同じレースに参加するには2人以上の出走準備が必要です。"
      });

      return;
    }

    const race =
      message.race || {};

    const fieldSize =
      Math.min(
        Math.max(
          Number(race.fieldSize) ||
            room.size,
          2
        ),
        12
      );

    const field = [];

    // プレイヤー馬を優先
    for (const p of readyPlayers) {

      if (field.length >= fieldSize) {
        break;
      }

      const h = p.horse || {};

      field.push({

        number:
          field.length + 1,

        playerId:
          p.id,

        playerName:
          p.name,

        isPlayer: true,

        name:
          String(
            h.name ||
            p.name ||
            "プレイヤー"
          ).slice(0, 12),

        speed:
          clampStat(h.speed),

        stamina:
          clampStat(h.stamina),

        power:
          clampStat(h.power),

        guts:
          clampStat(h.guts),

        wetAbility:
          clampStat(
            h.wetAbility ??
            h.rainAptitude ??
            70
          ),

        style:
          [
            "逃げ",
            "先行",
            "差し",
            "追込"
          ].includes(h.style)
            ? h.style
            : "先行"
      });
    }

    // =========================
    // CPU補充
    // =========================

    const names = [
      "サクラCPU",
      "ゴールドCPU",
      "ブルーCPU",
      "ダークCPU",
      "グリーンCPU",
      "レッドCPU",
      "ナイトCPU",
      "シルバーCPU",
      "サンダーCPU",
      "ホワイトCPU",
      "キングCPU",
      "スターCPU"
    ];

    while (field.length < fieldSize) {

      field.push({

        number:
          field.length + 1,

        playerId:
          null,

        playerName:
          "CPU",

        isPlayer:
          false,

        name:
          names[
            field.length %
            names.length
          ],

        speed:
          60 +
          Math.floor(
            Math.random() * 41
          ),

        stamina:
          60 +
          Math.floor(
            Math.random() * 41
          ),

        power:
          60 +
          Math.floor(
            Math.random() * 41
          ),

        guts:
          60 +
          Math.floor(
            Math.random() * 41
          ),

        wetAbility:
          60 +
          Math.floor(
            Math.random() * 41
          ),

        style:
          [
            "逃げ",
            "先行",
            "差し",
            "追込"
          ][
            Math.floor(
              Math.random() * 4
            )
          ]
      });
    }

    // オッズ計算
    calculateOnlineOdds(field);

    // レース計算
    const raceField =
      field.map(h => ({
        ...h,
        score: undefined
      }));

    const result =
      simulateRace(
        raceField,
        race
      );

    room.raceNo =
      (room.raceNo || 0) + 1;

    room.lastRace = {
      race:
        sanitizeRace(race),

      results:
        result
    };

    // 準備状態をリセット
    room.players.forEach(p => {

      p.ready = false;
      p.horse = null;

    });

    // =========================
    // レース開始通知
    // =========================

    broadcast(room, {

      type:
        "race_start",

      race:
        sanitizeRace(race),

      field:
        field.map(h => ({
          ...h,
          odds: h.odds
        })),

      players:
        publicPlayers(room)

    });

    // =========================
    // 1.8秒後に結果配信
    // =========================

    setTimeout(() => {

      if (!rooms.has(room.code)) {
        return;
      }

      broadcast(room, {

        type:
          "race_result",

        race:
          sanitizeRace(race),

        results:
          result,

        raceNo:
          room.raceNo

      });

      broadcastRoomState(room);

    }, 1800);

    return;
  }
}

// =========================
// ステータス値の安全化
// =========================

function clampStat(value) {

  const n = Number(value);

  return Number.isFinite(n)
    ? Math.max(
        1,
        Math.min(
          999,
          Math.round(n)
        )
      )
    : 70;
}

// =========================
// レース情報の安全化
// =========================

function sanitizeRace(race) {

  return {

    grade:
      String(
        race.grade || "ONLINE"
      ).slice(0, 20),

    name:
      String(
        race.name ||
        "オンラインレース"
      ).slice(0, 30),

    distance:
      Math.max(
        1000,
        Math.min(
          4000,
          Number(race.distance) ||
            1600
        )
      ),

    prize:
      Math.max(
        0,
        Number(race.prize) || 0
      ),

    fieldSize:
      Math.min(
        Math.max(
          Number(race.fieldSize) ||
            8,
          2
        ),
        12
      ),

    weather:
      race.weather
        ? {
            name:
              String(
                race.weather.name ||
                  "晴"
              ).slice(0, 8),

            rain:
              Math.max(
                0,
                Math.min(
                  1,
                  Number(
                    race.weather.rain
                  ) || 0
                )
              )
          }
        : {
            name: "晴",
            rain: 0
          },

    track:
      String(
        race.track || "良"
      ).slice(0, 8)
  };
}

// =========================
// オッズ計算
// =========================

function calculateOnlineOdds(field) {

  const abilities =
    field.map(h =>
      h.speed * 0.35 +
      h.stamina * 0.25 +
      h.power * 0.20 +
      h.guts * 0.20
    );

  const total =
    abilities.reduce(
      (a, b) => a + b,
      0
    ) || field.length;

  field.forEach((h, i) => {

    const raw =
      0.92 /
      (abilities[i] / total);

    h.odds =
      Math.max(
        1.5,
        Math.round(
          raw * 10
        ) / 10
      );
  });

  // 大穴を作る
  if (field.length >= 6) {

    const longshot =
      field.reduce(
        (a, b) =>
          a.odds > b.odds
            ? a
            : b
      );

    longshot.odds =
      Math.max(
        longshot.odds,
        100 +
          Math.floor(
            Math.random() * 81
          )
      );
  }
}

// =========================
// レースシミュレーション
// =========================

function simulateRace(
  field,
  race
) {

  const r =
    sanitizeRace(race);

  const scored =
    field.map(h => {

      let score =
        h.speed * 0.38 +
        h.stamina * 0.25 +
        h.power * 0.20 +
        h.guts * 0.17;

      if (h.style === "逃げ") {
        score += 5;
      }

      if (h.style === "差し") {
        score += 2;
      }

      if (h.style === "追込") {
        score += 3;
      }

      if (r.distance >= 2200) {

        score +=
          (h.stamina - 70) *
          0.10;
      }

      if (r.weather.rain > 0.3) {

        score +=
          (h.wetAbility - 70) *
          r.weather.rain *
          0.12;
      }

      if (r.track === "重") {

        score +=
          (h.wetAbility - 70) *
          0.08;
      }

      if (r.track === "不良") {

        score +=
          (h.wetAbility - 70) *
          0.13;
      }

      // ランダム要素
      score +=
        Math.random() * 35 -
        17.5;

      return {
        ...h,
        score
      };
    });

  scored.sort(
    (a, b) =>
      b.score - a.score
  );

  return scored.map(
    (h, i) => ({

      number:
        h.number,

      originalNumber:
        h.number,

      playerId:
        h.playerId,

      playerName:
        h.playerName,

      isPlayer:
        !!h.isPlayer,

      name:
        h.name,

      speed:
        h.speed,

      stamina:
        h.stamina,

      power:
        h.power,

      guts:
        h.guts,

      wetAbility:
        h.wetAbility,

      style:
        h.style,

      odds:
        h.odds,

      finishOrder:
        i + 1

    })
  );
}

// =========================
// HTTP
// =========================

const server =
  http.createServer(
    (req, res) => {

      if (
        req.url === "/" ||
        req.url === "/health"
      ) {

        res.writeHead(
          200,
          {
            "Content-Type":
              "text/plain; charset=utf-8"
          }
        );

        res.end(
          "KEIBA ONLINE SERVER OK"
        );

        return;
      }

      res.writeHead(
        404
      );

      res.end(
        "Not found"
      );
    }
  );

// =========================
// WebSocket
// =========================

server.on(
  "upgrade",
  (req, socket) => {

    if (
      String(
        req.headers.upgrade || ""
      ).toLowerCase() !==
      "websocket"
    ) {

      socket.destroy();

      return;
    }

    const key =
      req.headers[
        "sec-websocket-key"
      ];

    if (!key) {

      socket.destroy();

      return;
    }

    const accept =
      crypto
        .createHash("sha1")
        .update(
          key +
          "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        )
        .digest("base64");

    socket.write(
      "HTTP/1.1 101 Switching Protocols\r\n" +
      "Upgrade: websocket\r\n" +
      "Connection: Upgrade\r\n" +
      `Sec-WebSocket-Accept: ${accept}\r\n` +
      "\r\n"
    );

    socket.on(
      "data",
      parseWebSocketFrames(socket)
    );

    socket.on(
      "close",
      () => removePlayer(socket)
    );

    socket.on(
      "error",
      () => removePlayer(socket)
    );
  }
);

// =========================
// 起動
// =========================

server.listen(
  PORT,
  "0.0.0.0",
  () => {

    console.log(
      `KEIBA ONLINE SERVER listening on port ${PORT}`
    );

  }
);
