// Preserve diagram source so switching the Material palette redraws its colors.
(function () {
  var serial = 0;
  var pending = Promise.resolve();
  function render() {
    if (!window.mermaid) return;
    pending = pending.then(async function () {
      var theme = document.body.dataset.mdColorScheme === "slate" ? "dark" : "default";
      window.mermaid.initialize({
        startOnLoad: false,
        securityLevel: "strict",
        flowchart: {useMaxWidth: false, htmlLabels: true},
        sequence: {useMaxWidth: false},
        theme: theme,
      });
      for (var el of document.querySelectorAll("div.gnr-mermaid")) {
        if (el.dataset.theme === theme) continue;
        if (!el.dataset.source) el.dataset.source = el.textContent.trim();
        try {
          var result = await window.mermaid.render("gnr-d" + (++serial), el.dataset.source);
          el.innerHTML = result.svg;
          el.dataset.theme = theme;
        } catch (error) {
          var message = document.createElement("pre");
          message.textContent = "mermaid: " + String(error);
          el.replaceChildren(message);
        }
      }
    });
  }
  function start() {
    new MutationObserver(render).observe(document.body, {
      attributes: true, attributeFilter: ["data-md-color-scheme"]
    });
    render();
  }
  if (window.document$ && window.document$.subscribe) window.document$.subscribe(render);
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start);
  else start();
})();
