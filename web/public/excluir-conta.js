(() => {
  const form = document.getElementById('deletion-request-form');
  const contactType = document.getElementById('deletion-contact-type');
  const contact = document.getElementById('deletion-contact');
  const submit = document.getElementById('deletion-submit');
  const status = document.getElementById('deletion-form-status');
  const turnstileTarget = document.getElementById('deletion-turnstile');

  if (!form || !contactType || !contact || !submit || !status || !turnstileTarget) return;

  const config = window.__CENTRAL_CONFIG__?.privacyTurnstile || {};
  let turnstileToken = '';
  let turnstileWidgetId = null;

  const setStatus = (message, type = '') => {
    status.textContent = message;
    status.className = `form-status${type ? ` ${type}` : ''}`;
  };

  const updateContactInput = () => {
    const isEmail = contactType.value === 'email';
    contact.type = isEmail ? 'email' : 'tel';
    contact.autocomplete = isEmail ? 'email' : 'tel';
    contact.inputMode = isEmail ? 'email' : 'tel';
    contact.maxLength = isEmail ? 254 : 24;
    contact.placeholder = isEmail ? 'nome@exemplo.com' : '+55 65 99999-9999';
    contact.value = '';
  };

  const resetTurnstile = () => {
    turnstileToken = '';
    if (turnstileWidgetId !== null && window.turnstile?.reset) {
      window.turnstile.reset(turnstileWidgetId);
    }
  };

  const initializeTurnstile = () => {
    if (!config.enabled || !config.siteKey) {
      submit.disabled = true;
      setStatus(
        'O formulário está temporariamente indisponível. Use o e-mail ou WhatsApp acima.',
        'error'
      );
      return;
    }

    if (!window.turnstile?.render) {
      window.setTimeout(initializeTurnstile, 150);
      return;
    }

    turnstileWidgetId = window.turnstile.render(turnstileTarget, {
      sitekey: config.siteKey,
      action: config.action || 'privacy_deletion_request',
      callback: (token) => {
        turnstileToken = typeof token === 'string' ? token : '';
        setStatus('');
      },
      'expired-callback': resetTurnstile,
      'error-callback': () => {
        turnstileToken = '';
        setStatus('Não foi possível validar a proteção anti-robô. Tente novamente.', 'error');
      },
    });
  };

  contactType.addEventListener('change', updateContactInput);
  updateContactInput();

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const value = contact.value.trim();
    if (!value || !contact.checkValidity()) {
      contact.reportValidity();
      return;
    }
    if (!turnstileToken) {
      setStatus('Confirme a proteção anti-robô antes de enviar.', 'error');
      return;
    }

    submit.disabled = true;
    setStatus('Enviando solicitação...');

    try {
      const response = await fetch('/api/privacy/deletion-requests', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contactType: contactType.value,
          contact: value,
          turnstileToken,
        }),
      });
      const payload = await response.json().catch(() => ({}));

      if (response.status !== 202 || payload?.ok !== true) {
        throw new Error('request_failed');
      }

      contact.value = '';
      setStatus(
        'Solicitação recebida. Se os dados corresponderem a uma conta, continuaremos a verificação pelos canais oficiais.',
        'success'
      );
    } catch (_error) {
      setStatus(
        'Não foi possível enviar agora. Tente novamente ou use o e-mail ou WhatsApp acima.',
        'error'
      );
    } finally {
      resetTurnstile();
      submit.disabled = false;
    }
  });

  initializeTurnstile();
})();
