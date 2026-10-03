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

{{- define "obs.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{ include "obs.fullname" . }}
{{- else -}}
default
{{- end -}}
{{- end }}

{{- define "obs.image" -}}
{{- $reg := .root.Values.global.imageRegistry -}}
{{- if $reg -}}{{ trimSuffix "/" $reg }}/{{- end -}}{{ .image.repository }}:{{ .image.tag }}
{{- end }}

{{/*
Render-time guards. A misconfiguration here would otherwise deploy
successfully and fail quietly in the cluster.
*/}}
{{- define "obs.validate" -}}
{{- $p := .Values.processor -}}
{{- $parts := int .Values.kafka.rawTopicPartitions -}}
{{- if and $p.autoscaling.enabled $p.kedaScaling.enabled -}}
{{- fail "processor.autoscaling and processor.kedaScaling are both enabled; they would fight over replicas. Enable one." -}}
{{- end -}}
{{- if gt (int $p.replicaCount) $parts -}}
{{- fail (printf "processor.replicaCount (%d) exceeds kafka.rawTopicPartitions (%d); extra consumers would sit idle" (int $p.replicaCount) $parts) -}}
{{- end -}}
{{- if and $p.autoscaling.enabled (gt (int $p.autoscaling.maxReplicas) $parts) -}}
{{- fail (printf "processor.autoscaling.maxReplicas (%d) exceeds kafka.rawTopicPartitions (%d)" (int $p.autoscaling.maxReplicas) $parts) -}}
{{- end -}}
{{- if and $p.kedaScaling.enabled (gt (int $p.kedaScaling.maxReplicas) $parts) -}}
{{- fail (printf "processor.kedaScaling.maxReplicas (%d) exceeds kafka.rawTopicPartitions (%d)" (int $p.kedaScaling.maxReplicas) $parts) -}}
{{- end -}}
{{- if contains "/" (trimPrefix "https://" (trimPrefix "http://" $p.config.clickhouseUrl)) -}}
{{- fail "processor.config.clickhouseUrl must be a base URL without a path (e.g. http://clickhouse:8123); the database is set by the client" -}}
{{- end -}}
{{- $i := .Values.ingestor -}}
{{- $removeAfter := mul (int $i.probes.readiness.periodSeconds) (int $i.probes.readiness.failureThreshold) -}}
{{- if lt (int $i.config.drainDelaySeconds) $removeAfter -}}
{{- fail (printf "ingestor.config.drainDelaySeconds (%d) must be >= readiness periodSeconds x failureThreshold (%d), or the pod stops accepting before it leaves the Service" (int $i.config.drainDelaySeconds) $removeAfter) -}}
{{- end -}}
{{- if le (int $i.terminationGracePeriodSeconds) (add (int $i.config.drainDelaySeconds) 25) -}}
{{- fail (printf "ingestor.terminationGracePeriodSeconds (%d) must exceed drainDelaySeconds + 25s HTTP shutdown (%d)" (int $i.terminationGracePeriodSeconds) (add (int $i.config.drainDelaySeconds) 25)) -}}
{{- end -}}
{{- end }}
