{{- define "obs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "obs.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "obs.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: observability-pipeline
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}

{{- define "obs.selectorLabels" -}}
app.kubernetes.io/name: {{ include "obs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
