function parseUplink(device, payload) {
  const bytes = payload.asBytes();
  env.log("Payloadb:", bytes);

  const decoded = Decoder(bytes);
  env.log("Decoder:", decoded);

  // Reading
  if (decoded.reading !== undefined) {
    const sensor1 = device.endpoints.byAddress("1");
    if (sensor1 != null) {
      sensor1.updateVolumeSensorStatus(decoded.reading);
    }
  }

  // Battery
  if (decoded.battery_voltage !== undefined) {
    const sensor2 = device.endpoints.byAddress("2");
    if (sensor2 != null) {
      sensor2.updateVoltageSensorStatus(decoded.battery_voltage);
    }
  }

    // Valve Status
    if (decoded.valve_status !== undefined) {
    const sensor3 = device.endpoints.byAddress("3");
    if (sensor3 != null) {
        const position = decoded.valve_status === 'CLOSE' ? 0
                    : decoded.valve_status === 'Ready for reconnection' ? 90
                    : 100;

        sensor3.updateClosureControllerStatus(false, position); // Not moving, just reporting position
    }
  }
/*    // Historical profile data
    if (decoded.profiles !== undefined && Array.isArray(decoded.profiles)) {
    const sensor1 = device.endpoints.byAddress("1");
    if (sensor1 != null) {
        decoded.profiles.forEach(([timestamp, value]) => {
        sensor1.updateVolumeSensorStatus(parseFloat(value), new Date(timestamp));
        });
    }
  }*/
}

function buildDownlink(device, endpoint, command, payload) {
    payload.port = 1;
    payload.buildResult = downlinkBuildResult.ok;

    let meterId = device.address;

    if (!meterId || meterId.length < 12) {
        payload.buildResult = downlinkBuildResult.invalid;
        env.log("❌ Invalid or missing device address: " + meterId);
        return;
    }

    meterId = meterId.slice(-12);
    env.log("✅ Using meter ID: " + meterId);

    const baseHeader = [0xAA, 0x00];
    const addressBytes = hexStringToByteArray(meterId);
    const commandByte = 0x2F;
    const reservedBytes = [0x00, 0x00];
    const dataLength = 0x10;
    const systemTime = [0xFF, 0xFF, 0xFF, 0xFF];
    const commInterval = 0xFF;
    const nonCommParam = [0xFF, 0x00, 0x00, 0x00, 0x7f, 0x00, 0x00, 0x00, 0x7f, 0xff];

    let valveByte;
    let crc;

    switch (command.type) {
        case commandType.closure:
            switch (command.closure.type) {
                case closureCommandType.open:
                    valveByte = 0x00;
                    crc = [0xC4, 0x63]; // 🔓 OPEN → c463
                    env.log("🔓 Command: OPEN valve");
                    break;

                case closureCommandType.close:
                    valveByte = 0x01;
                    crc = [0x6C, 0x47]; // 🔒 CLOSE → 6c47
                    env.log("🔒 Command: CLOSE valve");
                    break;

                case closureCommandType.position:
                    if (command.closure.position === 0) {
                        valveByte = 0x00;
                        crc = [0xC4, 0x63];
                        env.log("🔓 Position 0% → OPEN valve");
                    } else if (command.closure.position === 100) {
                        valveByte = 0x01;
                        crc = [0x6C, 0x47];
                        env.log("🔒 Position 100% → CLOSE valve");
                    } else if (command.closure.position === 50) {
                        valveByte = 0x02;
                        crc = [0x00, 0x00]; // ⚠️ TEMP placeholder — update if needed
                        env.log("🟡 Position 50% → READY FOR RECONNECTION");
                    } else {
                        payload.buildResult = downlinkBuildResult.unsupported;
                        env.log("❌ Unsupported position: " + command.closure.position);
                        return;
                    }
                    break;

                default:
                    payload.buildResult = downlinkBuildResult.unsupported;
                    env.log("❌ Unsupported closure command type");
                    return;
            }

            const frame = [
                ...baseHeader,
                ...addressBytes,
                commandByte,
                ...reservedBytes,
                dataLength,
                valveByte,
                ...systemTime,
                commInterval,
                ...nonCommParam,
                ...crc // ✅ Injected CRC manually
            ];

            env.log("📦 Final payload: " + byteArrayToHex(frame));
            payload.setAsBytes(new Uint8Array(frame));
            break;

        default:
            payload.buildResult = downlinkBuildResult.unsupported;
            env.log("❌ Unsupported command type: " + command.type);
            break;
    }
}

// Utility: convert hex string to byte array
function hexStringToByteArray(hexString) {
    const bytes = [];
    for (let i = 0; i < hexString.length; i += 2) {
        bytes.push(parseInt(hexString.substr(i, 2), 16));
    }
    return bytes;
}

// Utility: convert byte array to hex string (for logging)
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

    function decodeStatus(statusLE) {
    const valveBits = statusLE & 0b11;  // bits 0 and 1

    const valve_status = valveBits === 0b01 || valveBits === 0b10
    ? 'CLOSE'
    : 'OPEN';

    const open_valve_disabled = ((statusLE >> 2) & 0b1) === 0;  // bit 2
    const tamper_status = ((statusLE >> 3) & 0b1) === 1;        // bit 3
    const battery_cover_alarm = ((statusLE >> 4) & 0b1) === 1;  // bit 4

    const alarm_code = [];
    if (valve_status !== 'OPEN') alarm_code.push('ValveAbnormal');
    if (tamper_status || battery_cover_alarm) alarm_code.push('BatteryCoverOpen');

    return {
        valve_status,
        open_valve_disabled,
        tamper_status,
        alarm_code: alarm_code.join(',')
    };
    }

  const msn = bytesU8.slice(2, 8).map(b => b.toString(16).padStart(2, '0')).join('');
  const frame_count = bytesU8[1];

  const timestamp = readUInt32BE(bytesU8, 12);
  const time = new Date(timestamp * 1000).toISOString().replace('.000Z', 'Z');

  const reading = decodeBCD4(bytesU8.slice(16, 20));

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

  const status = (bytesU8[47] << 8) | bytesU8[46];
  const {
    valve_status,
    open_valve_disabled,
    tamper_status,
    alarm_code
  } = decodeStatus(status);

  const battery_voltage = (bytesU8[48] * 2) / 100;

  const temperatures = [];
  const temp1 = bytesU8[44];
  const temp2 = bytesU8[45];
  const baseTime = new Date(timestamp * 1000);
  const tempTime1 = new Date(baseTime); tempTime1.setUTCHours(baseTime.getUTCHours());
  const tempTime2 = new Date(baseTime); tempTime2.setUTCHours(baseTime.getUTCHours() - 6);

  temperatures.push([tempTime1.toISOString().slice(0, 13) + ':00:00Z', temp1.toFixed(2)]);
  temperatures.push([tempTime2.toISOString().slice(0, 13) + ':00:00Z', temp2.toFixed(2)]);

  return {
    msn,
    time,
    reading,
    battery_voltage,
    valve_status,
    open_valve_disabled,
    tamper_status,
    alarm_code,
    temperatures,
    profiles,
    frame_count
  };
}