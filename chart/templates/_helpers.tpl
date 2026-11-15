{{/* helpers */}}
{{- define "hw11.name" -}}hw11{{- end -}}
{{- define "hw11.fullname" -}}hw11{{- end -}}
{{- define "hw11.image" -}}
{{- if .Values.real.enabled -}}
{{ printf "%s:%s" .Values.image.repository .Values.real.imageTag }}
{{- else -}}
{{ printf "%s:%s" .Values.image.repository .Values.image.tag }}
{{- end -}}
{{- end -}}
{{- define "hw11.workspace" -}}
{{- if .Values.real.enabled -}}{{ .Values.real.workspace }}{{- else -}}
/app/artifacts/sim/{{ .Values.modelVersion }}{{- end -}}
{{- end -}}
