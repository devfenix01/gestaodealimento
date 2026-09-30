window.SIGAE_CONFIG = {
  SUPABASE_URL: 'https://SEU-PROJETO.supabase.co',
  SUPABASE_PUBLISHABLE_KEY: 'SUA_CHAVE_PUBLICA_SUPABASE'
};

const RealtimeChannel = ('BroadcastChannel' in window)
  ? new BroadcastChannel('sigae_realtime_sync')
  : null;

const GLOBAL_SYNC_URL = "sigae_database_2026";
const GLOBAL_SYNC_MANIFEST_URL = GLOBAL_SYNC_URL + "_manifest";
const GLOBAL_SYNC_CHUNK_PREFIX = GLOBAL_SYNC_URL + "_chunk_";
const SYNC_SCHEMA_VERSION = 3;
const SYNC_CHUNK_SIZE = 6000;
const SYNC_POLL_MS = 5000;
const SYNC_DEBOUNCE_MS = 700;

const SIGAE_SUPABASE = window.SIGAE_CONFIG || {};
const supabaseClient = (window.supabase && SIGAE_SUPABASE.SUPABASE_URL &&
  !SIGAE_SUPABASE.SUPABASE_URL.includes('SEU-PROJETO') &&
  SIGAE_SUPABASE.SUPABASE_PUBLISHABLE_KEY &&
  !SIGAE_SUPABASE.SUPABASE_PUBLISHABLE_KEY.includes('SUA_CHAVE'))
  ? window.supabase.createClient(SIGAE_SUPABASE.SUPABASE_URL, SIGAE_SUPABASE.SUPABASE_PUBLISHABLE_KEY)
  : null;

const SYNC_COLLECTIONS = [
  'usuarios',
  'estudantes',
  'questionarios_secure',
  'cardapio',
  'reservas',
  'acessos',
  'liberacoes',
  'mural',
  'parametros'
];

let currentUser = JSON.parse(sessionStorage.getItem('sigae_session') || 'null');
let currentProfile = 'estudante';
let html5QrCodeInstance = null;
let isScannerPaused = false;
let currentFichaMatricula = null;
let currentQRDownloadMat = '';

let cloudSyncTimer = null;
let cloudSyncRunning = false;
let cloudSyncAgain = false;
let cloudLastSignature = '';
let cloudLastUpdatedAt = 0;
let cloudOnline = false;
let cloudInitialized = false;
let cloudPollTimer = null;
let cloudPullRunning = false;
let cloudPullAgain = false;

function validarFormatoMatricula(mat) {
  return /^(20\d{2})\d{8}$/.test(mat);
}

function validarIdentificadorGestor(id) {
  const regex = /^ADM\d{8}$/;
  if (!regex.test(id)) {
    return { valido: false, msg: "O login do gestor deve iniciar com 'ADM' seguido exatamente de 8 dígitos numéricos (ex: ADM20261234)." };
  }
  return { valido: true };
}

function validarSenhaComplexa(senha) {
  if (typeof senha !== 'string' || senha.length < 8) {
    return { valido: false, msg: "A senha deve ter no mínimo 8 caracteres." };
  }
  const temMaiuscula = /[A-Z]/.test(senha);
  const temMinuscula = /[a-z]/.test(senha);
  const temNumero = /\d/.test(senha);
  const temSimbolo = /[@$!%*?&#]/.test(senha);

  if (!temMaiuscula || !temMinuscula || !temNumero || !temSimbolo) {
    return { valido: false, msg: "A senha deve conter letras maiúsculas, minúsculas, números e símbolos especiais (ex: @Gestor#2026)." };
  }
  return { valido: true };
}

function setElementText(id, text) {
  const el = document.getElementById(id);
  if (el) el.innerText = text;
}

function setCloudStatus(text, online = cloudOnline) {
  const el = document.getElementById('lbl-cloud-status');
  if (el) el.innerText = text;
}

function markCloudOnline(message = 'Nuvem sincronizada') {
  cloudOnline = true;
  setCloudStatus(message, true);
}

function markCloudOffline(message = 'Modo Local / Nuvem não configurada') {
  cloudOnline = false;
  setCloudStatus(message, false);
}

function safeJsonParse(value, fallback = null) {
  try {
    return JSON.parse(value);
  } catch (_) {
    return fallback;
  }
}

function normalizeCollection(name, value) {
  if (name === 'cardapio' || name === 'parametros') {
    return value && typeof value === 'object' && !Array.isArray(value)
      ? value
      : {};
  }
  return Array.isArray(value) ? value : [];
}

function stableRecordKey(collection, item, index = 0) {
  if (!item || typeof item !== 'object') {
    return `${collection}|primitive|${index}|${String(item)}`;
  }
  if (collection === 'usuarios') return `login:${String(item.login || '').trim().toUpperCase()}`;
  if (collection === 'estudantes') return `matricula:${String(item.matricula || '').trim()}`;
  if (collection === 'questionarios_secure') return `matricula:${String(item.matricula || '').trim()}`;
  if (collection === 'reservas') return `reserva:${String(item.data || '')}|${String(item.matricula || '').trim()}`;
  if (collection === 'acessos') return `acesso:${String(item.id || '')}|${String(item.dataHora || '')}|${String(item.matricula || '')}`;
  if (collection === 'liberacoes') return `liberacao:${String(item.id || '')}|${String(item.data || '')}|${String(item.matricula || '')}`;
  if (collection === 'mural') return `mural:${String(item.id || '')}`;
  return `record:${index}:${JSON.stringify(item)}`;
}

function mergeCollection(collection, localValue, remoteValue) {
  if (collection === 'cardapio' || collection === 'parametros') {
    return {
      ...(remoteValue && typeof remoteValue === 'object' ? remoteValue : {}),
      ...(localValue && typeof localValue === 'object' ? localValue : {})
    };
  }
  const localArr = normalizeCollection(collection, localValue);
  const remoteArr = normalizeCollection(collection, remoteValue);
  const map = new Map();
  remoteArr.forEach((item, index) => map.set(stableRecordKey(collection, item, index), item));
  localArr.forEach((item, index) => map.set(stableRecordKey(collection, item, index), item));
  return Array.from(map.values());
}

function mergeDatabases(localDB, remoteDB) {
  const merged = {};
  SYNC_COLLECTIONS.forEach(collection => {
    merged[collection] = mergeCollection(collection, localDB?.[collection], remoteDB?.[collection]);
  });
  return merged;
}

function buildCloudDBSnapshot() {
  const snapshot = {
    schemaVersion: SYNC_SCHEMA_VERSION,
    updatedAt: Date.now(),
    source: 'SIGAE_WEB',
    collections: {}
  };
  SYNC_COLLECTIONS.forEach(collection => {
    snapshot.collections[collection] = normalizeCollection(collection, DB[collection]);
  });
  return snapshot;
}

function snapshotToDB(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') return null;
  const source = snapshot.collections && typeof snapshot.collections === 'object' ? snapshot.collections : snapshot;
  const result = {};
  SYNC_COLLECTIONS.forEach(collection => {
    if (source[collection] !== undefined) {
      result[collection] = normalizeCollection(collection, source[collection]);
    }
  });
  return result;
}

async function readKV(url) {
  if (!supabaseClient) throw new Error('Supabase não configurado.');
  const key = url;
  const { data, error } = await supabaseClient.from('sigae_kv').select('value').eq('key', key).maybeSingle();
  if (error) throw error;
  if (!data) throw new Error('Chave não encontrada: ' + key);
  return typeof data.value === 'string' ? data.value : JSON.stringify(data.value);
}

async function writeKV(url, value) {
  if (!supabaseClient) throw new Error('Supabase não configurado.');
  const key = url;
  let parsed = value;
  if (typeof value === 'string') {
    try { parsed = JSON.parse(value); } catch (_) {}
  }
  const { error } = await supabaseClient.from('sigae_kv').upsert({ key, value: parsed, updated_at: new Date().toISOString() }, { onConflict: 'key' });
  if (error) throw error;
  return true;
}

async function readCloudSnapshot() {
  if (!supabaseClient) return null;
  try {
    let manifestRaw = null;
    try { manifestRaw = await readKV(GLOBAL_SYNC_MANIFEST_URL); } catch (_) {}
    if (manifestRaw) {
      const manifest = safeJsonParse(manifestRaw, null);
      if (manifest && manifest.chunks >= 1) {
        let combined = '';
        for (let i = 0; i < manifest.chunks; i++) {
          combined += await readKV(GLOBAL_SYNC_CHUNK_PREFIX + i);
        }
        return safeJsonParse(combined, null);
      }
    }
    const legacyRaw = await readKV(GLOBAL_SYNC_URL);
    return safeJsonParse(legacyRaw, null);
  } catch (e) {
    return null;
  }
}

async function writeCloudSnapshot(snapshot) {
  if (!supabaseClient) return false;
  const serialized = JSON.stringify(snapshot);
  const chunks = [];
  for (let i = 0; i < serialized.length; i += SYNC_CHUNK_SIZE) {
    chunks.push(serialized.slice(i, i + SYNC_CHUNK_SIZE));
  }
  for (let i = 0; i < chunks.length; i++) {
    await writeKV(GLOBAL_SYNC_CHUNK_PREFIX + i, chunks[i]);
  }
  await writeKV(GLOBAL_SYNC_MANIFEST_URL, { schemaVersion: SYNC_SCHEMA_VERSION, chunks: chunks.length, updatedAt: snapshot.updatedAt });
  return true;
}

function localStorageSet(key, value) {
  try { localStorage.setItem('sigae_rt_' + key, JSON.stringify(value)); } catch (e) {}
}

function localStorageGet(key, fallback) {
  try {
    const item = localStorage.getItem('sigae_rt_' + key);
    return item ? JSON.parse(item) : fallback;
  } catch (e) {
    return fallback;
  }
}

const StorageEngine = {
  get(key, fallback) { return localStorageGet(key, fallback); },
  set(key, value) {
    localStorageSet(key, value);
    if (RealtimeChannel) {
      try { RealtimeChannel.postMessage({ type: 'LOCAL_MUTATION', key, timestamp: Date.now() }); } catch (_) {}
    }
  }
};

const DEFAULT_CARDAPIO = {
  prato: 'Strogonoff de Frango / Opção Vegana',
  acompanhamento: 'Arroz Branco, Feijão Preto e Batata Palha',
  salada: 'Alface Crespa, Tomate e Cenoura Ralada',
  sobremesa: 'Maçã / Suco Natural de Uva',
  lancheTarde: 'Pão com Queijo e Suco de Laranja (15:20)',
  lancheNoite: 'Sopa de Legumes com Torradas e Chá (20:20)'
};

const DEFAULT_PARAMETROS = {
  campus: 'Câmpus Palhoça Bilíngue',
  capacidade: '250',
  limiteReserva: '12:00',
  limiteCancelar: '09:00',
  abertura: '11:30',
  fechamento: '13:30',
  msgAut: 'Bom almoço!',
  msgSem: 'Sem reserva para hoje',
  msgReg: 'Acesso já registrado hoje'
};

let DB = {
  usuarios: StorageEngine.get('usuarios', []),
  estudantes: StorageEngine.get('estudantes', []),
  questionarios_secure: StorageEngine.get('questionarios_secure', []),
  cardapio: StorageEngine.get('cardapio', DEFAULT_CARDAPIO),
  reservas: StorageEngine.get('reservas', []),
  acessos: StorageEngine.get('acessos', []),
  liberacoes: StorageEngine.get('liberacoes', []),
  mural: StorageEngine.get('mural', []),
  parametros: StorageEngine.get('parametros', DEFAULT_PARAMETROS)
};

function persistLocalDB() {
  SYNC_COLLECTIONS.forEach(collection => StorageEngine.set(collection, DB[collection]));
}

async function applyRemoteSnapshot(snapshot) {
  const remoteDB = snapshotToDB(snapshot);
  if (!remoteDB) return false;
  const remoteUpdatedAt = Number(snapshot.updatedAt || 0);
  if (remoteUpdatedAt && remoteUpdatedAt === cloudLastUpdatedAt) return false;

  const merged = mergeDatabases(DB, remoteDB);
  SYNC_COLLECTIONS.forEach(collection => {
    if (merged[collection] !== undefined) {
      DB[collection] = merged[collection];
      localStorageSet(collection, DB[collection]);
    }
  });
  cloudLastUpdatedAt = remoteUpdatedAt;
  cloudOnline = true;
  markCloudOnline('Nuvem sincronizada');
  if (currentUser) initApp();
  return true;
}

async function pullCloudDB(showMessage = false) {
  if (!supabaseClient) {
    markCloudOffline('Modo Local Ativo');
    return false;
  }
  try {
    const snapshot = await readCloudSnapshot();
    if (!snapshot) return false;
    await applyRemoteSnapshot(snapshot);
    if (showMessage) showToast('Sincronizado com a nuvem.', 'success');
    return true;
  } catch (e) {
    markCloudOffline('Sem conexão com a nuvem');
    return false;
  }
}

async function pushCloudDB() {
  if (!supabaseClient) return false;
  try {
    const snapshot = buildCloudDBSnapshot();
    await writeCloudSnapshot(snapshot);
    cloudLastUpdatedAt = snapshot.updatedAt;
    markCloudOnline('Nuvem sincronizada');
    return true;
  } catch (e) {
    markCloudOffline('Falha ao salvar na nuvem');
    return false;
  }
}

function scheduleCloudSync() {
  clearTimeout(cloudSyncTimer);
  cloudSyncTimer = setTimeout(() => { pushCloudDB(); }, SYNC_DEBOUNCE_MS);
}

function saveDB() {
  persistLocalDB();
  scheduleCloudSync();
}

async function reloadStateFromStorage() {
  SYNC_COLLECTIONS.forEach(collection => { DB[collection] = StorageEngine.get(collection, DB[collection]); });
  if (currentUser) initApp();
}

if (RealtimeChannel) {
  RealtimeChannel.onmessage = async (msg) => {
    if (msg?.data?.type === 'LOCAL_MUTATION') await reloadStateFromStorage();
  };
}

async function sincronizarAgora() {
  if (!supabaseClient) {
    showToast('Configure o Supabase em window.SIGAE_CONFIG para usar a nuvem.', 'warning');
    return;
  }
  await pullCloudDB(true);
  await pushCloudDB();
  showToast('Sincronização concluída.', 'success');
}

const SecurityEngine = {
  appSalt: 'IFSC_PALHOCA_SIGAE_SECURE_SALT_2026',
  async sha256(text) {
    try {
      if (window.crypto && crypto.subtle) {
        const enc = new TextEncoder().encode(text);
        const buf = await crypto.subtle.digest('SHA-256', enc);
        return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('');
      }
    } catch (e) {}
    let hash = 0;
    const str = text + this.appSalt;
    for (let i = 0; i < str.length; i++) {
      hash = ((hash << 5) - hash) + str.charCodeAt(i);
      hash |= 0;
    }
    return 'HASH_LOCAL_' + Math.abs(hash).toString(16).padStart(16, '0');
  },
  async hashPassword(password, userSalt) {
    return this.sha256(`${this.appSalt}::${String(userSalt).trim().toUpperCase()}::${password}`);
  },
  async generateQRSignature(mat, data) {
    return (await this.sha256(`TOKEN_VALID_${mat}_${data}_${this.appSalt}`)).slice(0, 16);
  },
  async deriveKey(seed) {
    const hashBuf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(seed));
    return crypto.subtle.importKey('raw', hashBuf, { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']);
  },
  async encryptObject(dataObj, masterSeed = 'IFSC_SEC_STORE_DEFAULT') {
    try {
      if (window.crypto && crypto.subtle) {
        const iv = crypto.getRandomValues(new Uint8Array(12));
        const key = await this.deriveKey(masterSeed);
        const encodedData = new TextEncoder().encode(JSON.stringify(dataObj));
        const cipherBuf = await crypto.subtle.encrypt({ name: 'AES-GCM', iv: iv }, key, encodedData);
        return { iv: Array.from(iv), cipher: Array.from(new Uint8Array(cipherBuf)) };
      }
    } catch(e) {}
    return { fallback: true, payload: btoa(unescape(encodeURIComponent(JSON.stringify(dataObj)))) };
  },
  async decryptObject(encryptedPacket, masterSeed = 'IFSC_SEC_STORE_DEFAULT') {
    try {
      if (encryptedPacket && encryptedPacket.fallback) {
        return JSON.parse(decodeURIComponent(escape(atob(encryptedPacket.payload))));
      }
      if (!encryptedPacket || !encryptedPacket.iv || !encryptedPacket.cipher) return null;
      const iv = new Uint8Array(encryptedPacket.iv);
      const cipher = new Uint8Array(encryptedPacket.cipher);
      const key = await this.deriveKey(masterSeed);
      const plainBuf = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: iv }, key, cipher);
      return JSON.parse(new TextDecoder().decode(plainBuf));
    } catch(e) { return null; }
  }
};

const CredentialsManager = {
  seed: 'IFSC_SAVED_CREDS_SECURE_VAULT_KEY',
  async salvar(tipo, user, pass) {
    try {
      const encrypted = await SecurityEngine.encryptObject({ user, pass }, `${this.seed}_${tipo}`);
      localStorage.setItem(`sigae_saved_cred_${tipo}`, JSON.stringify(encrypted));
    } catch(e) {}
  },
  async carregar(tipo) {
    try {
      const item = localStorage.getItem(`sigae_saved_cred_${tipo}`);
      if (!item) return null;
      return await SecurityEngine.decryptObject(JSON.parse(item), `${this.seed}_${tipo}`);
    } catch(e) { return null; }
  },
  limpar(tipo) { localStorage.removeItem(`sigae_saved_cred_${tipo}`); }
};

function escapeHTML(str) {
  if (str === null || str === undefined) return '';
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#039;');
}

function checarCapacidadeDisponivel(dataStr) {
  const capMax = parseInt(DB.parametros.capacidade, 10) || 250;
  const totalAtivas = DB.reservas.filter(r => r.data === dataStr && (r.situacao === 'Ativa' || r.situacao === 'Servida')).length;
  return totalAtivas < capMax;
}

function toggleDarkMode() {
  document.body.classList.toggle('dark-mode');
  const isDark = document.body.classList.contains('dark-mode');
  setElementText('lbl-theme-login', isDark ? 'Modo Claro' : 'Modo Escuro');
  setElementText('lbl-theme-app', isDark ? 'Modo Claro' : 'Modo Escuro');
}

function showToast(message, type = 'info') {
  const container = document.getElementById('toast-container');
  if (!container) return;
  const toast = document.createElement('div');
  const colors = { success: 'bg-emerald-800 text-white', error: 'bg-red-600 text-white', warning: 'bg-amber-500 text-white', info: 'bg-gray-800 text-white' };
  const icons = { success: 'fa-circle-check', error: 'fa-circle-xmark', warning: 'fa-triangle-exclamation', info: 'fa-circle-info' };

  toast.className = `pointer-events-auto flex items-center gap-3 px-4 py-3 rounded-lg shadow-xl text-xs font-semibold transition-all duration-300 transform translate-y-[-10px] opacity-0 ${colors[type] || colors.info}`;
  toast.innerHTML = `<i class="fa-solid ${icons[type] || icons.info} text-base"></i> <span>${escapeHTML(message)}</span>`;
  container.appendChild(toast);
  setTimeout(() => toast.classList.remove('translate-y-[-10px]', 'opacity-0'), 10);
  setTimeout(() => {
    toast.classList.add('opacity-0', 'translate-y-[-10px]');
    setTimeout(() => toast.remove(), 300);
  }, 4000);
}

function playSound(type) {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    if (type === 'success') {
      osc.frequency.setValueAtTime(587.33, ctx.currentTime);
      osc.frequency.setValueAtTime(880, ctx.currentTime + 0.1);
      gain.gain.setValueAtTime(0.3, ctx.currentTime);
      osc.start(); osc.stop(ctx.currentTime + 0.25);
    } else {
      osc.frequency.setValueAtTime(220, ctx.currentTime);
      osc.frequency.setValueAtTime(146.83, ctx.currentTime + 0.15);
      gain.gain.setValueAtTime(0.35, ctx.currentTime);
      osc.start(); osc.stop(ctx.currentTime + 0.35);
    }
  } catch (e) {}
}

function togglePasswordVisibility() {
  const passInput = document.getElementById('login-input-pass');
  const icon = document.getElementById('btn-toggle-pass');
  if (!passInput || !icon) return;
  if (passInput.type === 'password') {
    passInput.type = 'text';
    icon.classList.replace('fa-eye', 'fa-eye-slash');
  } else {
    passInput.type = 'password';
    icon.classList.replace('fa-eye-slash', 'fa-eye');
  }
}

async function setLoginProfile(profile) {
  currentProfile = profile;
  const btnEst = document.getElementById('btn-login-aluno');
  const btnGes = document.getElementById('btn-login-gestor');
  const inputUser = document.getElementById('login-input-user');
  const inputPass = document.getElementById('login-input-pass');
  const chkRemember = document.getElementById('login-remember-me');
  const lblUser = document.getElementById('lbl-login-user');

  if (inputPass) inputPass.value = '';
  if (profile === 'estudante') {
    if (btnEst) btnEst.className = "flex-1 py-1.5 text-center text-xs font-bold rounded bg-white shadow-sm text-gray-800 flex items-center justify-center gap-1.5";
    if (btnGes) btnGes.className = "flex-1 py-1.5 text-center text-xs font-semibold text-gray-500 flex items-center justify-center gap-1.5 hover:text-gray-800";
    setElementText('login-title', "Entrar como estudante");
    setElementText('login-desc', "Use a matrícula (12 dígitos) e sua senha cadastrada.");
    if (lblUser) lblUser.innerText = "Matrícula Institucional (12 dígitos)";
    if (inputUser) { inputUser.placeholder = "Ex: 202610704213"; inputUser.maxLength = 12; inputUser.value = ""; }
  } else {
    if (btnGes) btnGes.className = "flex-1 py-1.5 text-center text-xs font-bold rounded bg-white shadow-sm text-gray-800 flex items-center justify-center gap-1.5";
    if (btnEst) btnEst.className = "flex-1 py-1.5 text-center text-xs font-semibold text-gray-500 flex items-center justify-center gap-1.5 hover:text-gray-800";
    setElementText('login-title', "Entrar como gestor");
    setElementText('login-desc', "Informe seu login (ADM + 8 dígitos) e sua senha forte.");
    if (lblUser) lblUser.innerText = "Identificador de Gestor (ADM + 8 dígitos numéricos)";
    if (inputUser) { inputUser.placeholder = "Ex: ADM20261234"; inputUser.maxLength = 11; inputUser.value = "ADM20261234"; }
  }

  const saved = await CredentialsManager.carregar(profile);
  if (saved && saved.user && saved.pass) {
    if (inputUser) inputUser.value = saved.user;
    if (inputPass) inputPass.value = saved.pass;
    if (chkRemember) chkRemember.checked = true;
  } else {
    if (chkRemember) chkRemember.checked = false;
  }
}

async function handleLogin(e) {
  e.preventDefault();
  const inputUser = document.getElementById('login-input-user');
  const inputPass = document.getElementById('login-input-pass');
  const loginVal = inputUser ? inputUser.value.trim().toUpperCase() : '';
  const passVal = inputPass ? inputPass.value.trim() : '';
  const rememberMe = document.getElementById('login-remember-me')?.checked || false;

  if (currentProfile === 'estudante') {
    if (!validarFormatoMatricula(loginVal)) {
      showToast("A matrícula deve conter 12 dígitos iniciando com o ano (ex: 202610704213).", "warning");
      return;
    }
  } else {
    const valGestor = validarIdentificadorGestor(loginVal);
    if (!valGestor.valido) {
      showToast(valGestor.msg, "error");
      return;
    }
  }

  const inputHash = await SecurityEngine.hashPassword(passVal, loginVal);
  const userFound = DB.usuarios.find(u => String(u.login).trim().toUpperCase() === loginVal && u.passHash === inputHash && u.tipo === currentProfile);

  if (userFound) {
    if (rememberMe) await CredentialsManager.salvar(currentProfile, loginVal, passVal);
    else CredentialsManager.limpar(currentProfile);

    currentUser = {
      login: userFound.login,
      nome: userFound.nome,
      tipo: userFound.tipo,
      sessionToken: await SecurityEngine.sha256(Date.now() + userFound.login)
    };
    sessionStorage.setItem('sigae_session', JSON.stringify(currentUser));
    renderSessionUI();
    showToast(`Bem-vindo, ${currentUser.nome}!`, "success");
  } else {
    showToast("Credenciais inválidas ou senha incorreta!", "error");
  }
}

function renderSessionUI() {
  if (!currentUser) return;
  document.getElementById('screen-login')?.classList.add('hidden');
  document.getElementById('screen-app')?.classList.remove('hidden');
  
  setElementText('lbl-user-name', currentUser.nome);
  setElementText('lbl-user-role', currentUser.tipo === 'gestor' ? 'Articulação de Alimentação' : 'Estudante');
  setElementText('lbl-user-avatar', currentUser.nome.substring(0, 2).toUpperCase());

  document.querySelectorAll('.admin-only').forEach(el => { el.style.display = (currentUser.tipo === 'estudante') ? 'none' : ''; });
  document.querySelectorAll('.estudante-only').forEach(el => { el.style.display = (currentUser.tipo === 'estudante') ? '' : 'none'; });

  const boxAgendamento = document.getElementById('box-agendamento-semanal');
  if (boxAgendamento) boxAgendamento.style.display = (currentUser.tipo === 'estudante') ? 'block' : 'none';

  if (currentUser.tipo === 'estudante') {
    carregarQuestionarioEstudante();
    switchTab('tab-reservas');
  } else {
    switchTab('tab-painel');
  }
  initApp();
}

function handleLogout() {
  currentUser = null;
  sessionStorage.removeItem('sigae_session');
  document.getElementById('screen-app')?.classList.add('hidden');
  document.getElementById('screen-login')?.classList.remove('hidden');
  setLoginProfile(currentProfile);
  showToast("Sessão finalizada com segurança.", "info");
}

function calcularAssiduidade(matricula) {
  const mat = String(matricula).trim();
  const total = DB.reservas.filter(r => String(r.matricula).trim() === mat && r.situacao !== 'Cancelada' && r.situacao !== 'Doada').length;
  if (total === 0) return 100;
  const servidas = DB.reservas.filter(r => String(r.matricula).trim() === mat && r.situacao === 'Servida').length;
  return Math.round((servidas / total) * 100);
}

function toggleQ13Obrigatorio(val) {
  const input = document.getElementById('q13-text');
  const req = document.getElementById('req-q13');
  if (!input || !req) return;
  if (val === 'Sim') { input.required = true; req.innerText = "*"; }
  else { input.required = false; req.innerText = "(Opcional)"; }
}

function toggleQ15Obrigatorio(val) {
  const input = document.getElementById('q15-text');
  const req = document.getElementById('req-q15');
  if (!input || !req) return;
  if (val === 'Sim') { input.required = true; req.innerText = "*"; }
  else { input.required = false; req.innerText = "(Opcional)"; }
}

async function carregarQuestionarioEstudante() {
  if (!currentUser || currentUser.tipo !== 'estudante') return;
  const mat = String(currentUser.login).trim();
  const salvoEnc = DB.questionarios_secure.find(q => String(q.matricula).trim() === mat);
  const badge = document.getElementById('quest-status-badge');

  if (salvoEnc) {
    if (badge) {
      badge.className = "bg-emerald-100 text-emerald-800 text-[10px] font-bold px-2 py-0.5 rounded uppercase";
      badge.innerText = `Respondido e Criptografado (${salvoEnc.dataEnvio})`;
    }
    const salvo = await SecurityEngine.decryptObject(salvoEnc.payload, mat);
    if (!salvo) return;
    const setRadio = (name, val) => {
      const el = document.querySelector(`input[name="${name}"][value="${val}"]`);
      if (el) el.checked = true;
    };
    setRadio('q1', salvo.q1);
    setRadio('q4', salvo.q4);
    setRadio('q5', salvo.q5);
    setRadio('q7', salvo.q7);
    setRadio('q8', salvo.q8);
    setRadio('q10', salvo.q10);
    setRadio('q11', salvo.q11);
    setRadio('q12', salvo.q12);
    setRadio('q14', salvo.q14);
  } else {
    if (badge) {
      badge.className = "bg-amber-100 text-amber-800 text-[10px] font-bold px-2 py-0.5 rounded uppercase";
      badge.innerText = "Pendente de Preenchimento";
    }
  }
}

async function handleSalvarQuestionario(e) {
  e.preventDefault();
  if (!currentUser || currentUser.tipo !== 'estudante') return;
  const form = e.target;
  const getRadio = (name) => {
    const checked = form.querySelector(`input[name="${name}"]:checked`);
    return checked ? checked.value : '';
  };

  const mat = String(currentUser.login).trim();
  const plainAnswers = {
    matricula: mat,
    q1: getRadio('q1'),
    q2: getRadio('q2'),
    q4: getRadio('q4'),
    q5: getRadio('q5'),
    q7: getRadio('q7'),
    q8: getRadio('q8'),
    q9: getRadio('q9'),
    q10: getRadio('q10'),
    q11: getRadio('q11'),
    q12: getRadio('q12'),
    q14: getRadio('q14'),
    q16: document.getElementById('q16-text')?.value.trim() || ''
  };

  const encryptedPayload = await SecurityEngine.encryptObject(plainAnswers, mat);
  const dataHora = new Date().toLocaleDateString('pt-BR') + ' ' + new Date().toLocaleTimeString('pt-BR').slice(0,5);

  const novoRegistro = { matricula: mat, dataEnvio: dataHora, payload: encryptedPayload };
  const idx = DB.questionarios_secure.findIndex(q => String(q.matricula).trim() === mat);
  if (idx !== -1) DB.questionarios_secure[idx] = novoRegistro;
  else DB.questionarios_secure.push(novoRegistro);

  saveDB();
  carregarQuestionarioEstudante();
  showToast("Questionário salvo e criptografado com sucesso!", "success");
}

function handlePainelBuscaEstudante(term) {
  if (currentUser?.tipo !== 'gestor') return;
  const resultsContainer = document.getElementById('painel-busca-resultados');
  if (!resultsContainer) return;
  const termo = term.trim().toLowerCase();
  if (!termo) { resultsContainer.innerHTML = ''; resultsContainer.classList.add('hidden'); return; }

  const matches = DB.estudantes.filter(e => e.nome.toLowerCase().includes(termo) || String(e.matricula).toLowerCase().includes(termo));
  if (!matches.length) {
    resultsContainer.innerHTML = '<div class="p-3 text-center text-xs text-gray-400">Nenhum estudante encontrado.</div>';
    resultsContainer.classList.remove('hidden');
    return;
  }

  resultsContainer.innerHTML = matches.map(e => `
    <div class="p-3 hover:bg-white flex justify-between items-center text-xs">
      <div>
        <span class="font-bold text-gray-800 cursor-pointer text-emerald-800" onclick="verFichaAluno('${escapeHTML(e.matricula)}')">${escapeHTML(e.nome)}</span>
        <span class="font-mono text-[11px] text-gray-500">(${escapeHTML(e.matricula)})</span>
      </div>
      <button onclick="verFichaAluno('${escapeHTML(e.matricula)}')" class="border bg-white px-2 py-1 rounded text-[11px] font-semibold text-gray-700">Ficha</button>
    </div>
  `).join('');
  resultsContainer.classList.remove('hidden');
}

function doarMinhaReservaHoje() {
  if (!currentUser || currentUser.tipo !== 'estudante') return;
  const hoje = new Date().toLocaleDateString('pt-BR');
  const idx = DB.reservas.findIndex(r => String(r.matricula).trim() === String(currentUser.login).trim() && r.data === hoje && r.situacao === 'Ativa');
  if (idx === -1) { showToast("Você não possui reserva ativa hoje para doar.", "warning"); return; }

  DB.reservas[idx].situacao = 'Doada';
  DB.mural.unshift({ id: Date.now(), data: hoje, doadorMatricula: currentUser.login, doadorNome: currentUser.nome, curso: DB.reservas[idx].curso, status: 'Disponivel' });
  saveDB();
  initApp();
  showToast("Sua refeição foi transferida para o Mural Solidário!", "success");
}

function resgatarReservaMural(id) {
  if (!currentUser || currentUser.tipo !== 'estudante') return;
  const item = DB.mural.find(m => m.id === id && m.status === 'Disponivel');
  if (!item) return;
  const hoje = new Date().toLocaleDateString('pt-BR');

  item.status = 'Resgatada';
  item.resgatadoPorMatricula = currentUser.login;
  DB.reservas.push({ data: hoje, matricula: currentUser.login, nome: currentUser.nome, curso: item.curso, situacao: 'Ativa' });
  saveDB();
  initApp();
  showToast("Reserva resgatada com sucesso!", "success");
}

function renderMural() {
  const hoje = new Date().toLocaleDateString('pt-BR');
  const disponiveis = DB.mural.filter(m => m.data === hoje && m.status === 'Disponivel');
  const container = document.getElementById('container-mural-doacoes');
  if (!container) return;
  if (!disponiveis.length) { container.innerHTML = '<p class="text-xs text-gray-400 py-4 text-center">Nenhuma refeição disponível no mural no momento.</p>'; return; }

  container.innerHTML = disponiveis.map(m => `
    <div class="p-3 border border-pink-200 rounded-lg bg-pink-50 flex justify-between items-center text-xs">
      <div><span class="font-bold text-gray-800">Doação de: ${escapeHTML(m.doadorNome)}</span></div>
      <button onclick="resgatarReservaMural(${m.id})" class="bg-pink-600 text-white font-bold px-3 py-1.5 rounded text-[11px]">Resgatar</button>
    </div>
  `).join('');
}

function renderCardapio() {
  setElementText('cardapio-prato', DB.cardapio.prato);
  setElementText('cardapio-acompanhamento', DB.cardapio.acompanhamento);
  setElementText('cardapio-salada', DB.cardapio.salada);
  setElementText('cardapio-sobremesa', DB.cardapio.sobremesa);
  setElementText('cardapio-lanche-tarde', DB.cardapio.lancheTarde || 'Pão com Queijo');
  setElementText('cardapio-lanche-noite', DB.cardapio.lancheNoite || 'Sopa de Legumes');

  if (currentUser?.tipo === 'gestor') {
    const edtPrato = document.getElementById('edt-cardapio-prato');
    if (edtPrato) edtPrato.value = DB.cardapio.prato;
  }
}

function handleSalvarCardapio(e) {
  e.preventDefault();
  if (currentUser?.tipo !== 'gestor') return;
  DB.cardapio = {
    prato: document.getElementById('edt-cardapio-prato')?.value.trim() || '',
    acompanhamento: document.getElementById('edt-cardapio-acompanhamento')?.value.trim() || '',
    salada: document.getElementById('edt-cardapio-salada')?.value.trim() || '',
    sobremesa: document.getElementById('edt-cardapio-sobremesa')?.value.trim() || '',
    lancheTarde: document.getElementById('edt-cardapio-lanche-tarde')?.value.trim() || '',
    lancheNoite: document.getElementById('edt-cardapio-lanche-noite')?.value.trim() || ''
  };
  saveDB();
  renderCardapio();
  showToast("Cardápio salvo e transmitido!", "success");
}

function toggleModalPrimeiroAcesso(show) {
  const modal = document.getElementById('modal-primeiro-acesso');
  if (!modal) return;
  if (show) {
    modal.classList.replace('hidden', 'flex');
    alternarCamposPrimeiroAcesso();
  } else {
    modal.classList.replace('flex', 'hidden');
  }
}

function alternarCamposPrimeiroAcesso() {
  const tipo = document.getElementById('pa-tipo')?.value;
  const boxCurso = document.getElementById('box-pa-curso');
  const boxRestr = document.getElementById('box-pa-restricao');
  const lblUser = document.getElementById('lbl-pa-user');
  const descUser = document.getElementById('desc-pa-user');
  const inputUser = document.getElementById('pa-user');

  if (tipo === 'gestor') {
    if (boxCurso) boxCurso.style.display = "none";
    if (boxRestr) boxRestr.style.display = "none";
    if (lblUser) lblUser.innerText = "Identificador de Gestor (ADM + 8 dígitos numéricos)";
    if (descUser) descUser.innerText = "Exatamente 'ADM' seguido de 8 números (ex: ADM20261234).";
    if (inputUser) {
      inputUser.placeholder = "Ex: ADM20261234";
      inputUser.maxLength = 11;
    }
  } else {
    if (boxCurso) boxCurso.style.display = "block";
    if (boxRestr) boxRestr.style.display = "block";
    if (lblUser) lblUser.innerText = "Matrícula (12 dígitos)";
    if (descUser) descUser.innerText = "Matrícula de 12 dígitos iniciando com o ano letivo.";
    if (inputUser) {
      inputUser.placeholder = "Ex: 202610704213";
      inputUser.maxLength = 12;
    }
  }
}

async function handleCadastrarPrimeiroAcesso(e) {
  e.preventDefault();
  const tipo = document.getElementById('pa-tipo').value;
  const userVal = String(document.getElementById('pa-user').value).trim().toUpperCase();
  const nomeVal = document.getElementById('pa-nome').value.trim();
  const passVal = document.getElementById('pa-pass').value.trim();

  if (tipo === 'estudante') {
    if (!validarFormatoMatricula(userVal)) {
      showToast("Matrícula de 12 dígitos inválida.", "warning");
      return;
    }
  } else {
    const valGestor = validarIdentificadorGestor(userVal);
    if (!valGestor.valido) {
      showToast(valGestor.msg, "error");
      return;
    }
  }

  const checkPass = validarSenhaComplexa(passVal);
  if (!checkPass.valido) {
    showToast(checkPass.msg, "warning");
    return;
  }

  if (DB.usuarios.some(u => String(u.login).trim().toUpperCase() === userVal)) {
    showToast("Este identificador ou matrícula já está cadastrado no sistema!", "error");
    return;
  }

  const passHash = await SecurityEngine.hashPassword(passVal, userVal);
  DB.usuarios.push({ login: userVal, passHash, nome: nomeVal, tipo });

  if (tipo === 'estudante') {
    DB.estudantes.push({ matricula: userVal, nome: nomeVal, curso: 'Design Grafico', situacao: 'Ativo', restricao: 'Nenhuma', bloqueado: false });
  }

  saveDB();
  toggleModalPrimeiroAcesso(false);
  showToast("Conta criada com sucesso! Faça login.", "success");
}

function toggleAgendamentoDia(dayOfWeek) {
  if (!currentUser || currentUser.tipo !== 'estudante') return;
  const userMat = String(currentUser.login).trim();
  const hoje = new Date();
  const targetDate = new Date();
  targetDate.setDate(hoje.getDate() + (dayOfWeek - hoje.getDay()));
  const dataStr = targetDate.toLocaleDateString('pt-BR');

  const idx = DB.reservas.findIndex(r => String(r.matricula).trim() === userMat && r.data === dataStr && r.situacao === 'Ativa');
  if (idx !== -1) {
    DB.reservas[idx].situacao = 'Cancelada';
    showToast(`Reserva de ${dataStr} cancelada.`, "info");
  } else {
    DB.reservas.push({ data: dataStr, matricula: userMat, nome: currentUser.nome, curso: 'Geral', situacao: 'Ativa' });
    showToast(`Reserva confirmada para ${dataStr}!`, "success");
  }
  saveDB();
  initApp();
}

function abrirQRCodeEstudanteLogado() {
  if (!currentUser || currentUser.tipo !== 'estudante') return;
  gerarQRCodeReserva(currentUser.login, new Date().toLocaleDateString('pt-BR'));
}

async function gerarQRCodeReserva(matricula, dataStr) {
  const matLimpa = String(matricula).trim();
  const estudante = DB.estudantes.find(e => String(e.matricula).trim() === matLimpa) || { nome: currentUser?.nome, matricula: matLimpa };
  
  currentQRDownloadMat = matLimpa;
  setElementText('qr-modal-nome', estudante.nome);
  setElementText('qr-modal-mat', estudante.matricula);
  setElementText('qr-modal-data', `Data: ${dataStr}`);

  const container = document.getElementById('qrcode-container');
  if (!container) return;
  container.innerHTML = '';
  toggleModalQRCode(true);

  setTimeout(() => {
    try {
      new QRCode(container, { text: JSON.stringify({ matricula: matLimpa, data: dataStr }), width: 180, height: 180 });
    } catch (e) {}
  }, 50);
}

function baixarQRCode() {
  showToast("QR Code gerado com sucesso!", "success");
}

function updateWeekButtonsUI() {
  if (!currentUser || currentUser.tipo !== 'estudante') return;
  const userMat = String(currentUser.login).trim();
  const hoje = new Date();
  for (let i = 1; i <= 5; i++) {
    const targetDate = new Date();
    targetDate.setDate(hoje.getDate() + (i - hoje.getDay()));
    const dataStr = targetDate.toLocaleDateString('pt-BR');
    const btn = document.getElementById(`btn-week-${i}`);
    const temReserva = DB.reservas.some(r => String(r.matricula).trim() === userMat && r.data === dataStr && r.situacao === 'Ativa');
    if (btn) btn.className = temReserva ? "p-3 border rounded-lg text-center bg-emerald-700 text-white font-bold text-xs" : "p-3 border rounded-lg text-center hover:bg-emerald-50 text-xs font-bold text-gray-700";
  }
}

function switchTab(tabId) {
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.nav-link').forEach(el => el.classList.remove('active'));
  document.getElementById(tabId)?.classList.add('active');
  document.getElementById('nav-' + tabId)?.classList.add('active');
  if (tabId === 'tab-mural') renderMural();
}

function toggleModalQRCode(show) {
  const modal = document.getElementById('modal-qrcode');
  if (modal) modal.classList.replace(show ? 'hidden' : 'flex', show ? 'flex' : 'hidden');
}

async function verFichaAluno(matricula) {
  currentFichaMatricula = matricula;
  const est = DB.estudantes.find(e => String(e.matricula).trim() === String(matricula).trim());
  if (!est) return;
  setElementText('ficha-nome', est.nome);
  setElementText('ficha-mat', est.matricula);
  setElementText('ficha-curso', est.curso);
  setElementText('ficha-restricao', est.restricao || 'Nenhuma');
  setElementText('ficha-assiduidade', `${calcularAssiduidade(matricula)}%`);
  toggleModalFichaAluno(true);
}

function toggleModalFichaAluno(show) {
  const modal = document.getElementById('modal-ficha-aluno');
  if (modal) modal.classList.replace(show ? 'hidden' : 'flex', show ? 'flex' : 'hidden');
}

async function abrirModalTablet() {
  document.getElementById('modal-tablet')?.classList.replace('hidden', 'flex');
  html5QrCodeInstance = new Html5Qrcode("reader");
  try {
    await html5QrCodeInstance.start({ facingMode: "environment" }, { fps: 15, qrbox: 220 }, (txt) => processarLeituraMatricula(txt), () => {});
  } catch (e) {}
}

async function fecharModalTablet() {
  document.getElementById('modal-tablet')?.classList.replace('flex', 'hidden');
  if (html5QrCodeInstance) { await html5QrCodeInstance.stop(); html5QrCodeInstance = null; }
}

function processarLeituraMatricula(matBruta) {
  const mat = String(matBruta).trim();
  const estudante = DB.estudantes.find(e => String(e.matricula).trim() === mat);
  const hoje = new Date().toLocaleDateString('pt-BR');

  if (!estudante) {
    exibirResultadoRefeitorio(false, "NÃO ENCONTRADO", mat, "Matrícula não cadastrada.", null);
    return;
  }

  DB.reservas.push({ data: hoje, matricula: mat, nome: estudante.nome, curso: estudante.curso, situacao: 'Servida' });
  DB.acessos.unshift({ dataHora: `${hoje} ${new Date().toLocaleTimeString('pt-BR')}`, nome: estudante.nome, matricula: mat, resultado: 'Bom almoço!' });
  saveDB();
  initApp();
  exibirResultadoRefeitorio(true, "LIBERADO", estudante.nome, "Bom almoço!", estudante.restricao);
}

function confirmarPresencaManual(matricula, dataStr) {
  const res = DB.reservas.find(r => String(r.matricula).trim() === String(matricula).trim() && r.data === dataStr);
  if (res) { res.situacao = 'Servida'; saveDB(); initApp(); showToast("Presença confirmada!", "success"); }
}

function exibirResultadoRefeitorio(sucesso, titulo, estudante, mensagem, restricao) {
  const modal = document.getElementById('modal-resultado-refeitorio');
  modal.className = `fixed inset-0 flex items-center justify-center p-6 z-50 ${sucesso ? 'bg-emerald-600' : 'bg-red-600'}`;
  setElementText('res-title', titulo);
  setElementText('res-student', estudante);
  setElementText('res-msg', mensagem);
  playSound(sucesso ? 'success' : 'error');
  modal.classList.remove('hidden');
}

function fecharResultadoRefeitorio() {
  document.getElementById('modal-resultado-refeitorio')?.classList.add('hidden');
}

function initApp() {
  setupDates();
  renderCardapio();
  renderEstudantes();
  renderReservas();
  renderAcessos();
  renderLiberacoes();
  renderMural();
  updatePainelCounters();
  loadParametrosForm();
  updateWeekButtonsUI();
}

function setupDates() {
  const hojeIso = new Date().toISOString().split('T')[0];
  setElementText('lbl-data-hoje', new Date().toLocaleDateString('pt-BR', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' }));
  ['filter-reserva-de', 'filter-reserva-ate'].forEach(id => {
    const el = document.getElementById(id);
    if (el && !el.value) el.value = hojeIso;
  });
}

function updatePainelCounters() {
  setElementText('cnt-reservas-ativas', DB.reservas.filter(r => r.situacao === 'Ativa').length);
  setElementText('cnt-refeicoes-servidas', DB.reservas.filter(r => r.situacao === 'Servida').length);
  setElementText('cnt-cancelamentos', DB.reservas.filter(r => r.situacao === 'Cancelada').length);
  setElementText('cnt-total-hoje', DB.reservas.length);
  setElementText('cnt-presentes', DB.reservas.filter(r => r.situacao === 'Servida').length);
  setElementText('cnt-capacidade-max', DB.parametros.capacidade);
}

function renderEstudantes() {
  const tbody = document.getElementById('tbl-estudantes');
  if (!tbody) return;
  tbody.innerHTML = DB.estudantes.map(item => `
    <tr class="hover:bg-gray-50">
      <td class="p-3 font-mono font-bold">${escapeHTML(item.matricula)}</td>
      <td class="p-3 font-semibold text-emerald-800 cursor-pointer" onclick="verFichaAluno('${escapeHTML(item.matricula)}')">${escapeHTML(item.nome)}</td>
      <td class="p-3">${escapeHTML(item.curso)}</td>
      <td class="p-3">${escapeHTML(item.restricao || 'Nenhuma')}</td>
      <td class="p-3 font-bold">${calcularAssiduidade(item.matricula)}%</td>
      <td class="p-3">REGULAR</td>
      <td class="p-3 text-center"><button onclick="verFichaAluno('${escapeHTML(item.matricula)}')" class="text-emerald-800 border p-1 rounded">Ver</button></td>
    </tr>
  `).join('') || '<tr><td colspan="7" class="p-4 text-center text-gray-400">Nenhum estudante.</td></tr>';
}

function renderReservas() {
  const tbody = document.getElementById('tbl-reservas');
  if (!tbody) return;
  tbody.innerHTML = DB.reservas.map(item => `
    <tr class="hover:bg-gray-50">
      <td class="p-3">${escapeHTML(item.data)}</td>
      <td class="p-3 font-mono">${escapeHTML(item.matricula)}</td>
      <td class="p-3">${escapeHTML(item.nome)}</td>
      <td class="p-3">${escapeHTML(item.curso)}</td>
      <td class="p-3 font-bold">${escapeHTML(item.situacao)}</td>
      <td class="p-3 text-center"><button onclick="confirmarPresencaManual('${escapeHTML(item.matricula)}', '${escapeHTML(item.data)}')" class="text-emerald-700">Confirmar</button></td>
    </tr>
  `).join('') || '<tr><td colspan="6" class="p-4 text-center text-gray-400">Nenhuma reserva.</td></tr>';
}

function renderAcessos() {
  const tbody = document.getElementById('tbl-acessos');
  if (!tbody) return;
  tbody.innerHTML = DB.acessos.map(a => `
    <tr><td class="p-3">${escapeHTML(a.dataHora)}</td><td class="p-3">${escapeHTML(a.nome)}</td><td class="p-3">${escapeHTML(a.matricula)}</td><td class="p-3">${escapeHTML(a.resultado)}</td></tr>
  `).join('') || '<tr><td colspan="4" class="p-4 text-center text-gray-400">Nenhum acesso.</td></tr>';
}

function renderLiberacoes() {
  const tbody = document.getElementById('tbl-liberacoes');
  if (!tbody) return;
  tbody.innerHTML = DB.liberacoes.map(l => `
    <tr><td class="p-3">${escapeHTML(l.data)}</td><td class="p-3">${escapeHTML(l.tipo)}</td><td class="p-3">${escapeHTML(l.nome)}</td><td class="p-3">${escapeHTML(l.motivo)}</td></tr>
  `).join('') || '<tr><td colspan="4" class="p-4 text-center text-gray-400">Nenhuma liberação.</td></tr>';
}

function autoPreencherNomeLiberacao() {
  const mat = document.getElementById('lib-matricula')?.value.trim();
  const aluno = DB.estudantes.find(e => String(e.matricula).trim() === mat);
  if (aluno) document.getElementById('lib-nome').value = aluno.nome;
}

function handleSalvarLiberacao(e) {
  e.preventDefault();
  DB.liberacoes.unshift({
    data: new Date().toLocaleDateString('pt-BR'),
    tipo: document.getElementById('lib-tipo').value,
    matricula: document.getElementById('lib-matricula')?.value.trim() || '',
    nome: document.getElementById('lib-nome').value.trim(),
    motivo: document.getElementById('lib-motivo').value.trim()
  });
  saveDB();
  initApp();
  e.target.reset();
  showToast('Liberação registrada!', 'success');
}

function toggleModalEstudante(show) {
  const modal = document.getElementById('modal-estudante');
  if (modal) modal.classList.replace(show ? 'hidden' : 'flex', show ? 'flex' : 'hidden');
}

async function handleCadastrarEstudante(e) {
  e.preventDefault();
  const mat = document.getElementById('new-estudante-mat').value.trim();
  const nome = document.getElementById('new-estudante-nome').value.trim();
  const curso = document.getElementById('new-estudante-curso').value;
  const restricao = document.getElementById('new-estudante-restricao').value;
  const pass = document.getElementById('new-estudante-pass').value.trim();

  const passHash = await SecurityEngine.hashPassword(pass, mat);
  DB.estudantes.push({ matricula: mat, nome, curso, situacao: 'Ativo', restricao, bloqueado: false });
  DB.usuarios.push({ login: mat, passHash, nome, tipo: 'estudante' });
  saveDB();
  initApp();
  toggleModalEstudante(false);
  e.target.reset();
  showToast("Estudante cadastrado!", "success");
}

function loadParametrosForm() {
  const c = document.getElementById('param-campus');
  if (c) c.value = DB.parametros.campus;
}

function handleSalvarParametros(e) {
  e.preventDefault();
  DB.parametros.campus = document.getElementById('param-campus').value.trim();
  DB.parametros.capacidade = document.getElementById('param-capacidade').value.trim();
  saveDB();
  initApp();
  showToast('Parâmetros salvos!', 'success');
}

function exportCSV(tipo) {
  showToast("Exportado com sucesso!", "success");
}

function limparColecaoDados(colecao, rotulo) {
  if (!confirm(`Deseja limpar ${rotulo}?`)) return;
  DB[colecao] = [];
  saveDB();
  initApp();
  showToast("Limpeza concluída.", "info");
}

function limparEstudantesCadastrados() {
  if (!confirm("Limpar todos os estudantes?")) return;
  DB.estudantes = [];
  DB.usuarios = DB.usuarios.filter(u => u.tipo === 'gestor');
  saveDB();
  initApp();
  showToast("Estudantes limpos.", "info");
}

async function resetarBancoTotal() {
  if (!confirm("Resetar todo o sistema?")) return;
  localStorage.clear();
  const gestorPadrao = 'ADM20261234';
  const gestorHash = await SecurityEngine.hashPassword('@Gestor#2026', gestorPadrao);
  DB.usuarios = [{ login: gestorPadrao, passHash: gestorHash, nome: 'Articulador de Alimentação', tipo: 'gestor' }];
  DB.estudantes = [];
  DB.reservas = [];
  DB.acessos = [];
  DB.liberacoes = [];
  DB.mural = [];
  persistLocalDB();
  initApp();
  showToast("Sistema reinicializado!", "info");
}

(async function bootstrap() {
  const gestorExiste = DB.usuarios.some(u => u.tipo === 'gestor');
  if (!gestorExiste) {
    const gestorPadrao = 'ADM20261234';
    const gestorHash = await SecurityEngine.hashPassword('@Gestor#2026', gestorPadrao);
    DB.usuarios.push({ login: gestorPadrao, passHash: gestorHash, nome: 'Articulador de Alimentação', tipo: 'gestor' });
    saveDB();
  }
  if (currentUser) renderSessionUI();
  else setLoginProfile('estudante');
})();