{{- define "quicksilver.name" -}}quicksilver{{- end -}}

{{- define "quicksilver.pluginName" -}}quicksilver.howlerops.io{{- end -}}

{{- define "quicksilver.labels" -}}
app.kubernetes.io/name: {{ include "quicksilver.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "quicksilver.selectorLabels" -}}
app.kubernetes.io/name: {{ include "quicksilver.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "quicksilver.serverSecret" -}}
{{- if .Values.certManager.enabled -}}quicksilver-server-tls{{- else -}}{{ required "existingSecrets.server is required when certManager.enabled is false" .Values.existingSecrets.server }}{{- end -}}
{{- end -}}

{{- define "quicksilver.clientSecret" -}}
{{- if .Values.certManager.enabled -}}quicksilver-client-tls{{- else -}}{{ required "existingSecrets.client is required when certManager.enabled is false" .Values.existingSecrets.client }}{{- end -}}
{{- end -}}
