SELECT
	"universities", "careers", "inscription_dates", "names", "sexo", "birth_dates",
	"locations", "emails"
FROM public.lat_sociales_cine
WHERE universities= '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES'
AND TO_DATE(inscription_dates,'DD-MM-YYYY') BETWEEN '01/09/2020' AND '01/02/2021'