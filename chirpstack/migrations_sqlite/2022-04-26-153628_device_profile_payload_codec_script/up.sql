alter table device_profile drop column payload_decoder_config;
alter table device_profile drop column payload_encoder_config;
alter table device_profile add column payload_codec_script text not null default '';

-- sqlite requires a complex procedure to remove the default of the column
-- see "simple procedure" in the second half of this section https://www.sqlite.org/lang_altertable.html#otheralter
-- alter table device_profile alter column payload_codec_script drop default;
