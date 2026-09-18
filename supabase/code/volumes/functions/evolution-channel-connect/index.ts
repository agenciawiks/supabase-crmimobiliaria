import 'jsr:@supabase/functions-js/edge-runtime.d.ts';
import { createClient } from 'npm:@supabase/supabase-js@2';

const WEBHOOK_URL =
  Deno.env.get('EVOLUTION_WEBHOOK_URL') ||
  'https://n8n-n8n.rh3fr2.easypanel.host/webhook/evolution-prod';
const DEFAULT_EVOLUTION_HOST =
  'n8n-evolution-api.rh3fr2.easypanel.host';
const REQUEST_TIMEOUT_MS = 7_000;
// Status polling must stay bounded, but the Evolution webhook endpoint may
// need a little more time than the connection-state endpoint.  Keeping both
// calls under the local Edge runtime budget prevents a stuck provider request
// from terminating the whole isolate.
const STATUS_REQUEST_TIMEOUT_MS = 4_000;

type EdgeRuntimeLike = {
  waitUntil?: (promise: Promise<unknown>) => void;
};

const edgeRuntime = (globalThis as typeof globalThis & {
  EdgeRuntime?: EdgeRuntimeLike;
}).EdgeRuntime;

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
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<{ response: Response; body: JsonRecord | null }> {
  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    timeoutMs,
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

  const isSuperAdmin = profile.is_super_admin === true;
  const requestedTenant = requestedTenantId
    ? validateUuid(requestedTenantId)
    : null;
  const profileTenantId = !isSuperAdmin && profile.tenant_id
    ? validateUuid(profile.tenant_id)
    : null;

  const tenantId = isSuperAdmin
    ? requestedTenant
    : requestedTenant ?? profileTenantId;

  if (!tenantId) {
    throw new HttpError(
      403,
      'TENANT_REQUIRED',
      'Selecione um cliente antes de conectar o canal.',
    );
  }

  if (!isSuperAdmin && tenantId !== profileTenantId) {
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
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<{ state: string; body: JsonRecord | null }> {
  const result = await evolutionRequest(
    `${evolutionUrl}/instance/connectionState/${encodeURIComponent(instance)}`,
    apiKey,
    {},
    timeoutMs,
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
): Promise<JsonRecord | null> {
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
  // Evolution 2.3.x may answer 403 when an instance name is already in use.
  // Reuse it here; the subsequent webhook request still validates the global
  // credential and will reject a genuinely unauthorized request.
  if (!result.response.ok && ![400, 403, 409].includes(result.response.status)) {
    evolutionFailure(
      result.response.status,
      'A Evolution API não conseguiu criar a instância.',
    );
  }

  return result.body;
}

async function readQrCodeFromEvolution(
  evolutionUrl: string,
  apiKey: string,
  instance: string,
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<string | null> {
  const result = await evolutionRequest(
    `${evolutionUrl}/instance/connect/${encodeURIComponent(instance)}`,
    apiKey,
    {},
    timeoutMs,
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
  timeoutMs = REQUEST_TIMEOUT_MS,
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
          'SEND_MESSAGE',
          'GROUPS_UPSERT',
          'GROUP_UPDATE',
          'GROUP_PARTICIPANTS_UPDATE',
        ],
        headers: {},
      }),
    },
    timeoutMs,
  );
  if (!webhookResult.response.ok) {
    evolutionFailure(
      webhookResult.response.status,
      'A Evolution API recusou a configuração do webhook.',
    );
  }

  const responseBody = webhookResult.body || {};
  const webhookConfig =
    (responseBody.webhook as JsonRecord | undefined) || responseBody;
  const returnedUrl = webhookConfig.url
    ? normalizeWebhookUrl(webhookConfig.url)
    : null;
  if (webhookConfig.enabled === false || (
    returnedUrl && returnedUrl !== normalizeWebhookUrl(WEBHOOK_URL)
  )) {
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
    webhookConfigured: boolean;
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
    webhook_url: values.webhookConfigured ? WEBHOOK_URL : '',
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

      // Managed channels must always use the server-side Evolution
      // credential. The key persisted on an older channel can be stale after
      // a VPS migration, while the global credential is the one that created
      // the instance and is still valid for webhook operations.
      let evolutionUrl: string;
      let apiKey: string;
      try {
        const managedConfig = getManagedEvolutionConfig();
        evolutionUrl = managedConfig.url;
        apiKey = managedConfig.apiKey;
      } catch {
        const storedApiKey = String(channel.api_key || '').trim();
        if (!storedApiKey) {
          throw new HttpError(
            422,
            'CHANNEL_CREDENTIALS_MISSING',
            'A credencial da instância não está disponível no CRM.',
          );
        }
        evolutionUrl = normalizeEvolutionUrl(String(channel.url || ''));
        apiKey = storedApiKey;
      }

      const instance = validateInstance(String(channel.instance || ''));
      // A URL stored in the database does not prove the provider still has
      // the webhook enabled (instances can be reset independently). Reapply
      // and verify it on every status check so existing channels self-heal.
      // Do not call /instance/connect here: that endpoint can hold the
      // request open while waiting for a new QR code and used to terminate
      // this Edge isolate before the webhook request completed. The QR code
      // is returned by the start action; this status path only checks state.
      const needsWebhook = true;
      const [stateResult, webhookResult] = await Promise.allSettled([
        readEvolutionState(
          evolutionUrl,
          apiKey,
          instance,
          false,
          STATUS_REQUEST_TIMEOUT_MS,
        ),
        ensureEvolutionWebhook(
          evolutionUrl,
          apiKey,
          instance,
          STATUS_REQUEST_TIMEOUT_MS,
        ),
      ]);

      if (stateResult.status === 'rejected') throw stateResult.reason;

      const providerConnected = stateResult.value.state === 'open';
      const webhookConfigured = webhookResult.status === 'fulfilled';
      // Existing channels are repaired synchronously here. This keeps the
      // response truthful: a channel is only reported ready after Evolution
      // confirms the webhook request, instead of trusting the DB URL alone.
      const connected = providerConnected && webhookConfigured;
      const nextStatus = connected ? 'connected' : 'disconnected';
      if (channel.status !== nextStatus || (needsWebhook && webhookConfigured)) {
        const updateValues: JsonRecord = {
          status: nextStatus,
          updated_at: new Date().toISOString(),
        };
        if (webhookConfigured) updateValues.webhook_url = WEBHOOK_URL;
        await supabase
          .from('channels')
          .update(updateValues)
          .eq('id', channel.id)
          .eq('tenant_id', tenantId);
      }

      return jsonResponse({
        success: true,
        action,
        channelId: channel.id,
        connected,
        state: stateResult.value.state,
        qrCode: null,
        webhookConfigured,
        webhookUrl: WEBHOOK_URL,
        message: connected
          ? 'WhatsApp conectado. O canal está pronto para receber mensagens.'
          : providerConnected
            ? 'WhatsApp conectado. Finalizando a configuração automática do webhook.'
            : 'A instância está aguardando conexão. Leia o QR Code exibido no CRM.',
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

    const creationBody = await createEvolutionInstance(
      evolutionUrl,
      apiKey,
      instance,
    );

    const creationState = readConnectionState(creationBody);
    const providerConnected = creationState === 'open';
    const qrCode = providerConnected ? null : readQrCode(creationBody);

    // Configure the provider before exposing the new channel. This is the
    // critical path for first-time connections; if the provider is slow, we
    // still persist the channel and the status poll will retry the same
    // idempotent request.
    let webhookConfigured = false;
    try {
      await ensureEvolutionWebhook(
        evolutionUrl,
        apiKey,
        instance,
        REQUEST_TIMEOUT_MS,
      );
      webhookConfigured = true;
    } catch (error) {
      console.error('[evolution-channel-connect] webhook setup failed', error);
    }

    const channel = await saveChannel(supabase, tenantId, {
      name,
      url: evolutionUrl,
      instance,
      apiKey,
      status: 'disconnected',
      webhookConfigured,
    });

    return jsonResponse({
      success: true,
      action: 'start',
      connected: false,
      state: creationState === 'unknown' ? 'connecting' : creationState,
      qrCode,
      channelId: channel.id,
      webhookConfigured,
      webhookUrl: WEBHOOK_URL,
      channel: {
        ...channel,
        api_key: undefined,
      },
      message: providerConnected
        ? webhookConfigured
          ? 'WhatsApp conectado. O webhook foi ativado automaticamente.'
          : 'WhatsApp conectado. Finalizando a configuração automática do webhook.'
        : qrCode
          ? webhookConfigured
            ? 'Instância criada. Leia o QR Code; o webhook já está ativo.'
            : 'Instância criada. Leia o QR Code enquanto o webhook é configurado automaticamente.'
          : 'Instância criada. O QR Code e o webhook serão sincronizados automaticamente.',
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
