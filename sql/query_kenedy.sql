SELECT
	"universidades", "carreras", "fechas_de_inscripcion", "nombres", "sexo", "fechas_nacimiento",
	"codigos_postales", "emails"
FROM public.uba_kenedy
WHERE universidades= 'universidad-j.-f.-kennedy'
AND TO_DATE(fechas_de_inscripcion,'YY-Mon-DD') BETWEEN '2020-Sep-01' AND '2021-Feb-01'