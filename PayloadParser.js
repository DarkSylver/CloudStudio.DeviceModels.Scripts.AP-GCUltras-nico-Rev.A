function parseUplink(device, payload) {
  const bytes = payload.asBytes();
  env.log("Payloadb:", bytes);

  const decoded = Decoder(bytes);
  env.log("Decoder:", decoded);

  // Reading -> ep 1
  if (decoded.reading !== undefined) {
    const sensor1 = device.endpoints.byAddress("1");
    if (sensor1 != null) {
      sensor1.updateVolumeSensorStatus(decoded.reading);
    }
  }

  // Battery -> ep 2
  if (decoded.battery_voltage !== undefined) {
    const sensor2 = device.endpoints.byAddress("2");
    if (sensor2 != null) {
      sensor2.updateVoltageSensorStatus(decoded.battery_voltage);
    }
  }

  // Valve Position -> ep 3 (usa valve_status NORMALIZADO)
  if (decoded.valve_status !== undefined) {
    const sensor3 = device.endpoints.byAddress("3");
    if (sensor3 != null) {
      const position =
        decoded.valve_status === 'CLOSE'
          ? 0
          : decoded.valve_status === 'Ready for reconnection'
          ? 90
          : 100;
      sensor3.updateClosureControllerStatus(false, position); // Not moving, just reporting position
    }
  }

  // --- Alertas/Estados a endpoints consecutivos ---

  /// 4: metrological_status
if (decoded.metrological_status !== undefined) {
  const ep = device.endpoints.byAddress("4");
  if (ep) ep.updateIASSensorStatus(decoded.metrological_status);
}

// 5: valv_status
if (decoded.valv_status !== undefined) {
  const ep = device.endpoints.byAddress("5");
  if (ep) ep.updateIASSensorStatus(decoded.valv_status);
}

// 6: valv_state
if (decoded.valv_state !== undefined) {
  const ep = device.endpoints.byAddress("6");
  if (ep) ep.updateIASSensorStatus(decoded.valv_state);
}

// 7: battery_status
if (decoded.battery_status !== undefined) {
  const ep = device.endpoints.byAddress("7");
  if (ep) ep.updateIASSensorStatus(decoded.battery_status);
}

// 8: battery_compartment_state
if (decoded.battery_compartment_state !== undefined) {
  const ep = device.endpoints.byAddress("8");
  if (ep) ep.updateIASSensorStatus(decoded.battery_compartment_state);
}

// 9: storage_status
if (decoded.storage_status !== undefined) {
  const ep = device.endpoints.byAddress("9");
  if (ep) ep.updateIASSensorStatus(decoded.storage_status);
}

// 10: overflow_state
if (decoded.overflow_state !== undefined) {
  const ep = device.endpoints.byAddress("10");
  if (ep) ep.updateIASSensorStatus(decoded.overflow_state);
}

// 11: reverse_flow_state
if (decoded.reverse_flow_state !== undefined) {
  const ep = device.endpoints.byAddress("11");
  if (ep) ep.updateIASSensorStatus(decoded.reverse_flow_state);
}

// 12: uncuncontrolled_flow_satatus (sic, nombre exacto de Tago). Si no viene, fallback al corregido.
if (decoded.uncuncontrolled_flow_satatus !== undefined || decoded.uncontrolled_flow_status !== undefined) {
  const ep = device.endpoints.byAddress("12");
  if (ep) {
    const val = (decoded.uncuncontrolled_flow_satatus !== undefined)
      ? decoded.uncuncontrolled_flow_satatus
      : decoded.uncontrolled_flow_status;
    ep.updateIASSensorStatus(val);
  }
}

// 13: temperature_sensor_state
if (decoded.temperature_sensor_state !== undefined) {
  const ep = device.endpoints.byAddress("13");
  if (ep) ep.updateIASSensorStatus(decoded.temperature_sensor_state);
}

// 14: clock_state
if (decoded.clock_state !== undefined) {
  const ep = device.endpoints.byAddress("14");
  if (ep) ep.updateIASSensorStatus(decoded.clock_state);
}
// 15: temperature
if (decoded.temperature !== undefined) {
  const ep = device.endpoints.byAddress("15");
  if (ep) ep.updateTemperatureSensorStatus(decoded.temperature);
}

  /* // Historical profile data (opcional)
  if (decoded.profiles !== undefined && Array.isArray(decoded.profiles)) {
    const sensor1 = device.endpoints.byAddress("1");
    if (sensor1 != null) {
      decoded.profiles.forEach(([timestamp, value]) => {
        sensor1.updateVolumeSensorStatus(parseFloat(value), new Date(timestamp));
      });
    }
  } */
}

function buildDownlink(device, endpoint, command, payload) {
  payload.port = 1;
  payload.buildResult = downlinkBuildResult.ok;

  function _hexToBytes(hex) {
    const clean = (hex || "").trim().toLowerCase();
    const out = [];
    for (let i = 0; i < clean.length; i += 2) out.push(parseInt(clean.substr(i, 2), 16));
    return out;
  }
  function _bytesToHex(arr) {
    return Array.from(arr).map(b => b.toString(16).padStart(2, "0")).join("");
  }

  // CRC-16/XMODEM (poly=0x1021, init=0x0000), sin reflect, xorout 0x0000.
  // Regla: calcular desde índice 1 (excluir 0xAA) y adjuntar en BE.
  function _crc16Xmodem(bytes) {
    let crc = 0x0000;
    const poly = 0x1021;
    for (let i = 1; i < bytes.length; i++) { // excluir 0xAA
      crc ^= (bytes[i] << 8);
      for (let j = 0; j < 8; j++) {
        crc = (crc & 0x8000) ? ((crc << 1) ^ poly) & 0xFFFF : (crc << 1) & 0xFFFF;
      }
    }
    return crc & 0xFFFF;
  }

  // meterId: exactamente 12 hex (últimos 12 si viene más largo)
  let meterId = (device.address || "").toString().trim();
  if (meterId.length > 12) meterId = meterId.slice(-12);
  if (!/^[0-9A-Fa-f]{12}$/.test(meterId)) {
    payload.buildResult = downlinkBuildResult.invalid;
    env.log("❌ Invalid meterId (need exactly 12 hex): " + meterId);
    return;
  }
  meterId = meterId.toLowerCase();

  // Solo closure open/close
  if (command.type !== commandType.closure) {
    payload.buildResult = downlinkBuildResult.unsupported;
    env.log("❌ Unsupported command type: " + command.type);
    return;
  }
  let valveByte;
  if (command.closure.type === closureCommandType.open) {
    valveByte = 0x00; // abrir
  } else if (command.closure.type === closureCommandType.close) {
    valveByte = 0x01; // cerrar
  } else {
    payload.buildResult = downlinkBuildResult.unsupported;
    env.log("❌ Only open/close are supported");
    return;
  }

  // Frame SIN CRC:
  // AA 01 | <meterId(6B)> | 2F 00 00 10 | <valveByte> |
  // FF FF FF FF FF FF | 00 00 00 7F | 00 00 00 7F | FF
  const frameNoCRC = [
    0xAA, 0x01,
    ..._hexToBytes(meterId),
    0x2F, 0x00, 0x00, 0x10,
    valveByte,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x00, 0x00, 0x00, 0x7F,
    0x00, 0x00, 0x00, 0x7F,
    0xFF
  ];

  // CRC sobre frame[1..end] y adjuntar BE
  const crc = _crc16Xmodem(frameNoCRC);
  const hi = (crc >> 8) & 0xFF;
  const lo = crc & 0xFF;

  const finalFrame = [...frameNoCRC, hi, lo];
  env.log("📦 Downlink HEX (XMODEM CRC): " + _bytesToHex(finalFrame));

  payload.setAsBytes(new Uint8Array(finalFrame));
}

// Utils opcionales
function hexStringToByteArray(hexString) {
  const bytes = [];
  for (let i = 0; i < hexString.length; i += 2) {
    bytes.push(parseInt(hexString.substr(i, 2), 16));
  }
  return bytes;
}
function byteArrayToHex(byteArray) {
  return byteArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

function Decoder(bytes) {
  const bytesU8 = new Uint8Array(bytes);

  function bcdToNumber(bcd) {
    return ((bcd >> 4) * 10) + (bcd & 0x0F);
  }
  function decodeBCD4(b) {
    return parseFloat((bcdToNumber(b[0]) * 10000 + bcdToNumber(b[1]) * 100 +
      bcdToNumber(b[2]) + bcdToNumber(b[3]) / 100).toFixed(2));
  }
  function decodeBCD2(b1, b2) {
    return parseFloat(((bcdToNumber(b1) * 100 + bcdToNumber(b2)) / 100).toFixed(2));
  }
  function readUInt32BE(b, i) {
    return (b[i] << 24) | (b[i + 1] << 16) | (b[i + 2] << 8) | b[i + 3];
  }

  // Campos base
  const msn = bytesU8.slice(2, 8).map(b => b.toString(16).padStart(2, '0')).join('');
  const frame_count = bytesU8[1];

  const timestamp = readUInt32BE(bytesU8, 12);
  const time = new Date(timestamp * 1000).toISOString().replace('.000Z', 'Z');

  const reading = decodeBCD4(bytesU8.slice(16, 20));

  // Perfiles 12h hacia atrás
  const profiles = [];
  let volume = reading;
  for (let i = 11; i >= 0; i--) {
    const offset = 20 + i * 2;
    const delta = decodeBCD2(bytesU8[offset], bytesU8[offset + 1]);
    volume = parseFloat((volume - delta).toFixed(2));

    const profileTime = new Date(timestamp * 1000);
    profileTime.setUTCHours(profileTime.getUTCHours() - (11 - i));
    const timeStr = profileTime.toISOString().slice(0, 13) + ':00:00Z';

    profiles.push([timeStr, volume.toFixed(2)]);
  }

  // Temperaturas
  const temperatures = [];
  const temp1 = bytesU8[44];
  const temp2 = bytesU8[45];
  const baseTime = new Date(timestamp * 1000);
  const tempTime1 = new Date(baseTime); tempTime1.setUTCHours(baseTime.getUTCHours());
  const tempTime2 = new Date(baseTime); tempTime2.setUTCHours(baseTime.getUTCHours() - 6);
  temperatures.push([tempTime1.toISOString().slice(0, 13) + ':00:00Z', temp1.toFixed(2)]);
  temperatures.push([tempTime2.toISOString().slice(0, 13) + ':00:00Z', temp2.toFixed(2)]);
  const temperature = temp1;  

  // -------- Status (LE) replicando Tago EXACTO --------
  const status = (bytesU8[47] << 8) | bytesU8[46];
  const meter_status = status.toString(2).padStart(16, '0');
  const bitStr  = (n) => meter_status.charAt(15 - n);     // bit N → charAt(15-N)
  const twoBits = (hi, lo) => bitStr(hi) + bitStr(lo);

const metrological_status = (bitStr(15) === '1') ? 2 : 1;

// Válvula (bits 14..13). Texto (para EP3) + código IAS para "lógico"
const vPair = twoBits(14, 13);
const valve_status_text =
  vPair === '00' ? 'OPEN' :
  vPair === '01' ? 'CLOSE' :
  vPair === '10' ? 'Ready for reconnection' : 'OPEN'; // fallback OPEN

// valv_status (IAS): Open -> 1 (idle), Close/Ready/Abnormal -> 2 (active)
const valv_status = (vPair === '00') ? 1 : 2;

// valv_state: bit 12 (1=Abnormal->2, 0=Normal->1)
const valv_state = (bitStr(12) === '1') ? 2 : 1;

// battery_status (bits 11..10)
const bPair = twoBits(11, 10);
const battery_status =
  (bPair === '00') ? 1 :              // Normal -> idle
  (bPair === '01') ? 7 :              // 10% remaining -> maintenanceNeeded
  (bPair === '10') ? 2 :              // Less than 5% -> active
                    2;               // otro -> active

// battery_compartment_state: bit 9 (1=Abnormal->6 tampered, 0=Normal->1)
const battery_compartment_state = (bitStr(9) === '1') ? 6 : 1;

// storage_status: bit 8 (1=Abnormal->2, 0=Normal->1)
const storage_status = (bitStr(8) === '1') ? 2 : 1;

// overflow_state: bit 7 (1=Abnormal->2, 0=Normal->1)
const overflow_state = (bitStr(7) === '1') ? 2 : 1;

// reverse_flow_state: bit 6 (1=Abnormal->2, 0=Normal->1)
const reverse_flow_state = (bitStr(6) === '1') ? 2 : 1;

// uncuncontrolled_flow_satatus (sic): bit 5 (1=Abnormal->2, 0=Normal->1)
const uncuncontrolled_flow_satatus = (bitStr(5) === '1') ? 2 : 1;

// temperature_sensor_state: bit 4 (1=Abnormal->2, 0=Normal->1)
const temperature_sensor_state = (bitStr(4) === '1') ? 2 : 1;

// clock_state: bit 3 (1=Need UTC ->2, 0=Not calibration ->1)
const clock_state = (bitStr(3) === '1') ? 2 : 1;
  // valve_status NORMALIZADO para tu endpoint 3 (CLOSE / Ready for reconnection / OPEN)
  let valve_status;
  if (vPair === '01') valve_status = 'CLOSE';
  else if (vPair === '10') valve_status = 'Ready for reconnection';
  else valve_status = 'OPEN'; // 00 u otros → OPEN

  const battery_voltage = (bytesU8[48] * 2) / 100;

  return {
    msn,
    time,
    reading,
    battery_voltage,

    // Para tu endpoint 3 (posición)
    valve_status,

    // Para endpoints 4..14 (idéntico a Tago)
    metrological_status,
    valv_status,
    valv_state,
    battery_status,
    battery_compartment_state,
    storage_status,
    overflow_state,
    reverse_flow_state,
    uncuncontrolled_flow_satatus, // (sic)
    temperature_sensor_state,
    clock_state,

    // extra que ya usabas
    temperature,
    temperatures,
    profiles,
    frame_count
  };
}

