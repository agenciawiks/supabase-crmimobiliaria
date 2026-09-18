import 'jsr:@supabase/functions-js/edge-runtime.d.ts';
import { createClient } from 'npm:@supabase/supabase-js@2';

const WEBHOOK_URL =
  Deno.env.get('EVOLUTION_WEBHOOK_URL') ||
  'https://n8n-n8n.rh3fr2.easypanel.host/webhook/evolution-prod';
const DEFAULT_EVOLUTION_HOST =
  'n8n-evolution-api.rh3fr2.easypanel.host';
const REQUEST_TIMEOUT_MS = 12_000;

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers':
    'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

type ConnectPayload = {
  action?: 'start' | 'status';
  name?: string;
  channelId?: string;
  tenantId?: string;
};

type JsonRecord = Record<string, unknown>;

class HttpError extends Error {
  status: number;
  code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

function jsonResponse(
  body: JsonRecord,
  status = 200,
): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      ...corsHeaders,
      'Content-Type': 'application/json',
      'Cache-Control': 'no-store',
    },
  });
}

function normalizeEvolutionUrl(value: string): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new HttpError(
      400,
      'INVALID_EVOLUTION_URL',
      'A URL da Evolution API é inválida.',
    );
  }

  if (
    parsed.protocol !== 'https:' ||
    parsed.username ||
    parsed.password
  ) {
    throw new HttpError(
      400,
      'INVALID_EVOLUTION_URL',
      'Use uma URL HTTPS válida, sem usuário ou senha.',
    );
  }

  const configuredHosts = (
    Deno.env.get('EVOLUTION_ALLOWED_HOSTS') ||
    DEFAULT_EVOLUTION_HOST
  )
    .split(',')
    .map((host) => host.trim().toLowerCase())
    .filter(Boolean);

  if (!configuredHosts.includes(parsed.hostname.toLowerCase())) {
    throw new HttpError(
      400,
      'EVOLUTION_HOST_NOT_ALLOWED',
      'Este servidor Evolution API não está autorizado.',
    );
  }

  return `${parsed.protocol}//${parsed.host}`;
}

function validateInstance(value: string): string {
  const instance = value.trim();
  if (!/^[a-zA-Z0-9._-]{2,80}$/.test(instance)) {
    throw new HttpError(
      400,
      'INVALID_INSTANCE',
      'O nome da instância contém caracteres inválidos.',
    );
  }
  return instance;
}

function createManagedInstanceName(tenantId: string, channelName: string): string {
  const tenantPart = tenantId.replace(/-/g, '').slice(0, 10).toLowerCase();
  const namePart = channelName
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 48) || 'whatsapp';
  return validateInstance(`crm-${tenantPart}-${namePart}`.slice(0, 80));
}

function getManagedEvolutionConfig(): { url: string; apiKey: string } {
  const apiKey = String(
    Deno.env.get('EVOLUTION_API_KEY') ||
      Deno.env.get('AUTHENTICATION_API_KEY') ||
      '',
  ).trim();
  if (!apiKey) {
    throw new HttpError(
      500,
      'SERVER_CONFIGURATION_ERROR',
      'A conexão automática da Evolution ainda não foi configurada no servidor.',
    );
  }
  const url = normalizeEvolutionUrl(
    String(
      Deno.env.get('EVOLUTION_API_URL') ||
        `https://${DEFAULT_EVOLUTION_HOST}`,
    ).trim(),
  );
  return { url, apiKey };
}

function validateUuid(value: unknown): string {
  const uuid = String(value || '');
  if (
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      uuid,
    )
  ) {
    throw new HttpError(
      403,
      'TENANT_NOT_FOUND',
      'O usuário autenticado não possui um tenant válido.',
    );
  }
  return uuid;
}

async function evolutionRequest(
  url: string,
  apiKey: string,
  init: RequestInit = {},
): Promise<{ response: Response; body: JsonRecord | null }> {
  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    REQUEST_TIMEOUT_MS,
  );

  try {
    const response = await fetch(url, {
      ...init,
      signal: controller.signal,
      headers: {
        apikey: apiKey,
        Accept: 'application/json',
        ...(init.body ? { 'Content-Type': 'application/json' } : {}),
        ...(init.headers || {}),
      },
    });
    const body = await response.json().catch(() => null);
    return { response, body };
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') {
      throw new HttpError(
        504,
        'EVOLUTION_TIMEOUT',
        'A Evolution API não respondeu dentro do tempo limite.',
      );
    }
    throw new HttpError(
      502,
      'EVOLUTION_UNREACHABLE',
      'Não foi possível alcançar a Evolution API.',
    );
  } finally {
    clearTimeout(timeout);
  }
}

function evolutionFailure(
  status: number,
  fallback: string,
): never {
  if (status === 401 || status === 403) {
    throw new HttpError(
      422,
      'INVALID_EVOLUTION_CREDENTIALS',
      'A Evolution API recusou a chave informada.',
    );
  }
  if (status === 404) {
    throw new HttpError(
      422,
      'EVOLUTION_INSTANCE_NOT_FOUND',
      'A instância informada não existe na Evolution API.',
    );
  }
  throw new HttpError(502, 'EVOLUTION_ERROR', fallback);
}

async function resolveAuthorizedTenant(
  supabase: ReturnType<typeof createClient>,
  userId: string,
  requestedTenantId: unknown,
): Promise<string> {
  const { data: profile, error: profileError } = await supabase
    .from('profiles')
    .select('tenant_id,is_super_admin,is_active')
    .eq('id', userId)
    .maybeSingle();

  if (profileError || !profile) {
    throw new HttpError(
      403,
      'PROFILE_NOT_FOUND',
      'O perfil autenticado não foi encontrado.',
    );
  }
  if (profile.is_active === false) {
    throw new HttpError(
      403,
      'PROFILE_INACTIVE',
      'Este usuário não está ativo para conectar canais.',
    );
  }

  const profileTenantId = profile.tenant_id
    ? validateUuid(profile.tenant_id)
    : null;
  let tenantId: string;
  if (requestedTenantId) {
    tenantId = validateUuid(requestedTenantId);
  } else if (profileTenantId) {
    tenantId = profileTenantId;
  } else {
    throw new HttpError(
      403,
      'TENANT_REQUIRED',
      'Selecione um cliente antes de conectar o canal.',
    );
  }

  if (
    profile.is_super_admin !== true &&
    (!profileTenantId || tenantId !== profileTenantId)
  ) {
    throw new HttpError(
      403,
      'TENANT_ACCESS_DENIED',
      'Você não possui acesso para conectar canais neste cliente.',
    );
  }

  const { data: tenant, error: tenantError } = await supabase
    .from('tenants')
    .select('id')
    .eq('id', tenantId)
    .maybeSingle();
  if (tenantError || !tenant) {
    throw new HttpError(
      404,
      'TENANT_NOT_FOUND',
      'O cliente selecionado não foi encontrado.',
    );
  }

  return tenantId;
}

function normalizeWebhookUrl(value: unknown): string {
  return String(value || '').replace(/\/+$/, '');
}

function readConnectionState(body: JsonRecord | null): string {
  const stateBody = body || {};
  const stateContainer =
    (stateBody.instance as JsonRecord | undefined) || stateBody;
  return String(
    stateContainer.state ||
      stateContainer.status ||
      stateContainer.connectionStatus ||
      'unknown',
  ).toLowerCase();
}

function readQrCode(body: JsonRecord | null): string | null {
  const root = body || {};
  const nested = (root.qrcode as JsonRecord | undefined) ||
    (root.qrCode as JsonRecord | undefined) || {};
  const candidate = [
    root.base64,
    typeof root.qrCode === 'string' ? root.qrCode : null,
    typeof root.qrcode === 'string' ? root.qrcode : null,
    nested.base64,
    nested.image,
    nested.code,
  ].find((value) => typeof value === 'string' && value.trim());

  if (typeof candidate !== 'string' || !candidate.trim()) return null;
  const value = candidate.trim();
  if (value.startsWith('data:image/')) return value;
  if (/^[A-Za-z0-9+/=\r\n]+$/.test(value) && value.length > 80) {
    return `data:image/png;base64,${value.replace(/\s+/g, '')}`;
  }
  return null;
}

async function readEvolutionState(
  evolutionUrl: string,
  apiKey: string,
  instance: string,
  allowMissing = false,
): Promise<{ state: string; body: JsonRecord | null }> {
  const result = await evolutionRequest(
    `${evolutionUrl}/instance/connectionState/${encodeURIComponent(instance)}`,
    apiKey,
  );
  if (!result.response.ok) {
    if (allowMissing && result.response.status === 404) {
      return { state: 'not_found', body: result.body };
    }
    evolutionFailure(
      result.response.status,
      'A Evolution API não conseguiu consultar a instância.',
    );
  }
  return { state: readConnectionState(result.body), body: result.body };
}

async function createEvolutionInstance(
  evolutionUrl: string,
  apiKey: string,
  instance: string,
): Promise<void> {
  const result = await evolutionRequest(
    `${evolutionUrl}/instance/create`,
    apiKey,
    {
      method: 'POST',
      body: JSON.stringify({
        instanceName: instance,
        integration: 'WHATSAPP-BAILEYS',
        qrcode: true,
      }),
    },
  );

  // Evolution returns 4xx when the instance already exists. In that case we
  // reuse it and continue with webhook/QR synchronization instead of creating
  // a duplicate connection.
  if (!result.response.ok && ![400, 409].includes(result.response.status)) {
    evolutionFailure(
      result.response.status,
      'A Evolution API não conseguiu criar a instância.',
    );
  }
}

async function readQrCodeFromEvolution(
  evolutionUrl: string,
  apiKey: string,
  instance: string,
): Promise<string | null> {
  const result = await evolutionRequest(
    `${evolutionUrl}/instance/connect/${encodeURIComponent(instance)}`,
    apiKey,
  );
  if (!result.response.ok) {
    if ([404, 409].includes(result.response.status)) return null;
    evolutionFailure(
      result.response.status,
      'A Evolution API não conseguiu gerar o QR Code.',
    );
  }
  return readQrCode(result.body);
}

async function ensureEvolutionWebhook(
  evolutionUrl: string,
  apiKey: string,
  instance: string,
): Promise<void> {
  const webhookResult = await evolutionRequest(
    `${evolutionUrl}/webhook/set/${encodeURIComponent(instance)}`,
    apiKey,
    {
      method: 'POST',
      body: JSON.stringify({
        enabled: true,
        url: WEBHOOK_URL,
        webhookByEvents: false,
        webhookBase64: false,
        events: [
          'CONNECTION_UPDATE',
          'MESSAGES_UPSERT',
          'MESSAGES_UPDATE',
          'SEND_MESSAGE_UPDATE',
          'GROUPS_UPSERT',
          'GROUP_UPDATE',
          'GROUP_PARTICIPANTS_UPDATE',
        ],
        headers: {},
      }),
    },
  );
  if (!webhookResult.response.ok) {
    evolutionFailure(
      webhookResult.response.status,
      'A Evolution API recusou a configuração do webhook.',
    );
  }

  const verifyResult = await evolutionRequest(
    `${evolutionUrl}/webhook/find/${encodeURIComponent(instance)}`,
    apiKey,
  );
  if (!verifyResult.response.ok) {
    evolutionFailure(
      verifyResult.response.status,
      'Não foi possível confirmar o webhook configurado.',
    );
  }

  const verifyBody = verifyResult.body || {};
  const webhookConfig =
    (verifyBody.webhook as JsonRecord | undefined) || verifyBody;
  const configuredUrl = normalizeWebhookUrl(webhookConfig.url);
  if (
    webhookConfig.enabled === false ||
    configuredUrl !== normalizeWebhookUrl(WEBHOOK_URL)
  ) {
    throw new HttpError(
      502,
      'WEBHOOK_VERIFICATION_FAILED',
      'A Evolution API não confirmou o webhook correto.',
    );
  }
}

async function saveChannel(
  supabase: ReturnType<typeof createClient>,
  tenantId: string,
  values: {
    name: string;
    url: string;
    instance: string;
    apiKey: string;
    status: string;
  },
): Promise<JsonRecord> {
  const channelValues = {
    tenant_id: tenantId,
    name: values.name,
    provider: 'evolution',
    status: values.status,
    url: values.url,
    instance: values.instance,
    api_key: values.apiKey,
    webhook_url: WEBHOOK_URL,
    updated_at: new Date().toISOString(),
  };

  const { data: existingChannel, error: lookupError } = await supabase
    .from('channels')
    .select('id')
    .eq('tenant_id', tenantId)
    .eq('provider', 'evolution')
    .eq('instance', values.instance)
    .maybeSingle();

  if (lookupError) {
    throw new HttpError(
      500,
      'CHANNEL_LOOKUP_FAILED',
      'Não foi possível consultar o canal no CRM.',
    );
  }

  const saveQuery = existingChannel?.id
    ? supabase
        .from('channels')
        .update(channelValues)
        .eq('id', existingChannel.id)
        .eq('tenant_id', tenantId)
    : supabase.from('channels').insert(channelValues);

  const { data: channel, error: saveError } = await saveQuery
    .select(
      'id,name,provider,status,url,instance,webhook_url,tenant_id,created_at,updated_at',
    )
    .single();

  if (saveError || !channel) {
    throw new HttpError(
      500,
      'CHANNEL_SAVE_FAILED',
      'A API foi validada, mas o canal não pôde ser salvo no CRM.',
    );
  }
  return channel as JsonRecord;
}

async function handleRequest(request: Request): Promise<Response> {
  if (request.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return jsonResponse(
      {
        success: false,
        code: 'METHOD_NOT_ALLOWED',
        message: 'Método não permitido.',
      },
      405,
    );
  }

  try {
    const authorization = request.headers.get('Authorization') || '';
    const accessToken = authorization.replace(/^Bearer\s+/i, '').trim();
    if (!accessToken) {
      throw new HttpError(
        401,
        'UNAUTHENTICATED',
        'Faça login novamente para conectar o canal.',
      );
    }

    const supabaseUrl = Deno.env.get('SUPABASE_URL');
    const serviceRoleKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
    if (!supabaseUrl || !serviceRoleKey) {
      throw new HttpError(
        500,
        'SERVER_CONFIGURATION_ERROR',
        'A função não está configurada corretamente.',
      );
    }

    const supabase = createClient(supabaseUrl, serviceRoleKey, {
      auth: { persistSession: false, autoRefreshToken: false },
    });
    const {
      data: { user },
      error: authError,
    } = await supabase.auth.getUser(accessToken);

    if (authError || !user) {
      throw new HttpError(
        401,
        'UNAUTHENTICATED',
        'Sua sessão expirou. Faça login novamente.',
      );
    }

    const payload = (await request.json().catch(() => null)) as
      | ConnectPayload
      | null;
    const tenantId = await resolveAuthorizedTenant(
      supabase,
      user.id,
      payload?.tenantId,
    );
    const action = payload?.action || 'start';

    if (action === 'status') {
      const channelId = String(payload?.channelId || '').trim();
      if (!channelId) {
        throw new HttpError(
          400,
          'INVALID_CHANNEL_ID',
          'O canal da Evolution não foi identificado.',
        );
      }

      const { data: channel, error: channelError } = await supabase
        .from('channels')
        .select(
          'id,name,provider,status,url,instance,api_key,webhook_url,tenant_id,created_at,updated_at',
        )
        .eq('id', channelId)
        .eq('tenant_id', tenantId)
        .eq('provider', 'evolution')
        .maybeSingle();

      if (channelError || !channel) {
        throw new HttpError(
          404,
          'CHANNEL_NOT_FOUND',
          'O canal da Evolution não foi encontrado para este cliente.',
        );
      }

      const apiKey = String(channel.api_key || '').trim();
      if (!apiKey) {
        throw new HttpError(
          422,
          'CHANNEL_CREDENTIALS_MISSING',
          'A credencial da instância não está disponível no CRM.',
        );
      }

      const evolutionUrl = normalizeEvolutionUrl(String(channel.url || ''));
      const instance = validateInstance(String(channel.instance || ''));
      const stateResult = await readEvolutionState(
        evolutionUrl,
        apiKey,
        instance,
      );
      const connected = stateResult.state === 'open';
      const qrCode = connected
        ? null
        : await readQrCodeFromEvolution(evolutionUrl, apiKey, instance);

      if (normalizeWebhookUrl(channel.webhook_url) !== normalizeWebhookUrl(WEBHOOK_URL)) {
        await ensureEvolutionWebhook(evolutionUrl, apiKey, instance);
      }

      const nextStatus = connected ? 'connected' : 'disconnected';
      if (channel.status !== nextStatus || normalizeWebhookUrl(channel.webhook_url) !== normalizeWebhookUrl(WEBHOOK_URL)) {
        await supabase
          .from('channels')
          .update({ status: nextStatus, webhook_url: WEBHOOK_URL, updated_at: new Date().toISOString() })
          .eq('id', channel.id)
          .eq('tenant_id', tenantId);
      }

      return jsonResponse({
        success: true,
        action,
        channelId: channel.id,
        connected,
        state: stateResult.state,
        qrCode,
        webhookConfigured: true,
        webhookUrl: WEBHOOK_URL,
        message: connected
          ? 'WhatsApp conectado. O canal está pronto para receber mensagens.'
          : qrCode
            ? 'Aguardando a leitura do QR Code pelo WhatsApp.'
            : 'A instância está aguardando conexão. Atualize o QR Code para tentar novamente.',
      });
    }

    if (action !== 'start') {
      throw new HttpError(400, 'INVALID_ACTION', 'Ação de conexão inválida.');
    }

    const name = String(payload?.name || '').trim();

    if (!name || name.length > 120) {
      throw new HttpError(
        400,
        'INVALID_CHANNEL_NAME',
        'Informe um nome válido para o canal.',
      );
    }
    const { url: evolutionUrl, apiKey } = getManagedEvolutionConfig();
    const instance = createManagedInstanceName(tenantId, name);

    const currentState = await readEvolutionState(
      evolutionUrl,
      apiKey,
      instance,
      true,
    );
    if (currentState.state === 'not_found') {
      await createEvolutionInstance(evolutionUrl, apiKey, instance);
    }

    // Configure and verify the inbound route before returning anything to the
    // browser. This prevents a QR from being shown for a channel that cannot
    // mirror messages back to the CRM.
    await ensureEvolutionWebhook(evolutionUrl, apiKey, instance);

    const stateAfterCreate = await readEvolutionState(
      evolutionUrl,
      apiKey,
      instance,
      true,
    );
    const connected = stateAfterCreate.state === 'open';
    const qrCode = connected
      ? null
      : await readQrCodeFromEvolution(evolutionUrl, apiKey, instance);
    const status = connected ? 'connected' : 'disconnected';
    const channel = await saveChannel(supabase, tenantId, {
      name,
      url: evolutionUrl,
      instance,
      apiKey,
      status,
    });

    return jsonResponse({
      success: true,
      action: 'start',
      connected,
      state: stateAfterCreate.state,
      qrCode,
      channelId: channel.id,
      webhookConfigured: true,
      webhookUrl: WEBHOOK_URL,
      channel: {
        ...channel,
        api_key: undefined,
      },
      message: connected
        ? 'Evolution API conectada e webhook configurado.'
        : qrCode
          ? 'Instância criada. Leia o QR Code para concluir a conexão.'
          : 'Instância criada e webhook configurado. O QR Code ainda não está disponível; atualize para tentar novamente.',
    });
  } catch (error) {
    if (error instanceof HttpError) {
      return jsonResponse(
        {
          success: false,
          code: error.code,
          message: error.message,
        },
        error.status,
      );
    }

    console.error('[evolution-channel-connect]', error);
    return jsonResponse(
      {
        success: false,
        code: 'UNEXPECTED_ERROR',
        message: 'Não foi possível concluir a conexão.',
      },
      500,
    );
  }
}

export default {
  fetch: handleRequest,
};
