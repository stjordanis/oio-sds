/*
OpenIO SDS meta2v2
Copyright (C) 2021 OVH SAS

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <glib.h>

#include <metautils/lib/metautils.h>

#include <meta2v2/meta2_utils_sharding.h>

static GError*
_shard_range_decode(struct json_object *jshard_range,
		struct shard_range_s **pshard_range)
{
	GError *err = NULL;

	struct json_object *jindex = NULL, *jlower = NULL, *jupper = NULL,
			*jcid = NULL;
	struct oio_ext_json_mapping_s mapping[] = {
		{"index", &jindex, json_type_int,    1},
		{"lower", &jlower, json_type_string, 1},
		{"upper", &jupper, json_type_string, 1},
		{"cid",   &jcid,   json_type_string, 1},
		{NULL,NULL,0,0}
	};
	err = oio_ext_extract_json(jshard_range, mapping);
	if (err) {
		goto end;
	}

	struct shard_range_s *shard_range = g_malloc0(
			sizeof(struct shard_range_s));
	shard_range->index = json_object_get_int(jindex);
	shard_range->lower = g_strdup(json_object_get_string(jlower));
	shard_range->upper = g_strdup(json_object_get_string(jupper));
	shard_range->cid = g_strdup(json_object_get_string(jcid));
	*pshard_range = shard_range;

end:
	if (err)
		g_prefix_error(&err, "Failed to decode shard range: ");
	return err;
}

static void
_shard_range_encode(struct shard_range_s *shard_range,
		GString *shard_range_json)
{
	g_string_append_c(shard_range_json, '{');
	oio_str_gstring_append_json_pair_int(shard_range_json, "index",
			shard_range->index);
	g_string_append_c(shard_range_json, ',');
	oio_str_gstring_append_json_pair(shard_range_json, "lower",
			shard_range->lower);
	g_string_append_c(shard_range_json, ',');
	oio_str_gstring_append_json_pair(shard_range_json, "upper",
			shard_range->upper);
	g_string_append_c(shard_range_json, ',');
	oio_str_gstring_append_json_pair(shard_range_json, "cid",
			shard_range->cid);
	g_string_append_c(shard_range_json, '}');
}

static void
_shard_range_free(struct shard_range_s *shard_range)
{
	if (!shard_range)
		return;

	g_free(shard_range->lower);
	g_free(shard_range->upper);
	g_free(shard_range->cid);
	g_free(shard_range);
}

static gint
_shard_range_cmp(gconstpointer a, gconstpointer b, gpointer u UNUSED)
{
	const struct shard_range_s *shard_range1 = a;
	const struct shard_range_s *shard_range2 = b;

	if (shard_range1->index < shard_range2->index) {
		return -1;
	} else if (shard_range1->index == shard_range2->index) {
		return 0;
	} else {
		return 1;
	}
}

GError*
shard_ranges_decode(const gchar *str, shard_ranges_t *pshard_ranges)
{
	GError *err = NULL;
	GTree *shard_ranges = NULL;

	struct json_tokener *tok = json_tokener_new();
	struct json_object *jshard_ranges = json_tokener_parse_ex(tok,
			str, strlen(str));
	json_tokener_free(tok);
	if (!jshard_ranges) {
		err = BADREQ("Parse error");
		goto end;
	}

	shard_ranges = g_tree_new_full(_shard_range_cmp, NULL, NULL,
			(GDestroyNotify)_shard_range_free);
	int nb_shard_ranges = json_object_array_length(jshard_ranges);
	for (int i = 0; i < nb_shard_ranges; i++) {
		struct json_object *jshard = json_object_array_get_idx(
				jshard_ranges, i);
		struct shard_range_s *shard = NULL;
		err = _shard_range_decode(jshard, &shard);
		if (err)
			goto end;
		g_tree_insert(shard_ranges, shard, shard);
	}

	*pshard_ranges = shard_ranges;

end:
	json_object_put(jshard_ranges);
	if (err) {
		shard_ranges_free(shard_ranges);
		g_prefix_error(&err, "Failed to decode shard ranges: ");
	}
	return err;
}

static gboolean
_shard_ranges_encode(gpointer key UNUSED, gpointer value, gpointer data) {
	GString *shard_ranges_json = data;
	if (shard_ranges_json->str[shard_ranges_json->len - 1] != '[')
		g_string_append_c(shard_ranges_json, ',');
	_shard_range_encode(value, shard_ranges_json);
	return FALSE;
}

gchar*
shard_ranges_encode(shard_ranges_t shard_ranges)
{
	GString *shard_ranges_json = g_string_new("[");
	g_tree_foreach(shard_ranges, _shard_ranges_encode, shard_ranges_json);
	g_string_append_c(shard_ranges_json, ']');
	return g_string_free(shard_ranges_json, FALSE);
}

static gint
_shard_check_range(const gchar *lower, const gchar *upper, const gchar *path)
{
	EXTRA_ASSERT(lower != NULL);
	EXTRA_ASSERT(upper != NULL);
	EXTRA_ASSERT(path != NULL);

	if (*lower && strncmp(path, lower, LIMIT_LENGTH_CONTENTPATH) <= 0) {
		return -1;
	}
	if (*upper && strncmp(path, upper, LIMIT_LENGTH_CONTENTPATH) > 0) {
		return 1;
	}
	// lower < path <= upper
	return 0;
}

static gint
_shard_range_cmp_with_path(gconstpointer a, gconstpointer b)
{
	const struct shard_range_s *shard_range = a;
	const gchar *path = b;

	return _shard_check_range(shard_range->lower, shard_range->upper, path);
}

struct shard_range_s *
shard_ranges_get_shard_range(shard_ranges_t shards, const gchar *path)
{
	if (!shards)
		return NULL;

	return g_tree_search(shards, _shard_range_cmp_with_path, path);
}

void
shard_ranges_free(shard_ranges_t shard_ranges)
{
	if (shard_ranges)
		g_tree_destroy(shard_ranges);
}

gchar*
shard_info_encode(struct shard_info_s *shard_info)
{
	EXTRA_ASSERT(shard_info != NULL);

	GString *shard_info_json = g_string_new("{");
	oio_str_gstring_append_json_pair(shard_info_json, "root_cid",
			shard_info->root_cid);
	g_string_append_c(shard_info_json, ',');
	oio_str_gstring_append_json_pair_int(shard_info_json, "timestamp",
			shard_info->timestamp);
	g_string_append_c(shard_info_json, ',');
	oio_str_gstring_append_json_pair(shard_info_json, "lower",
			shard_info->lower);
	g_string_append_c(shard_info_json, ',');
	oio_str_gstring_append_json_pair(shard_info_json, "upper",
			shard_info->upper);
	g_string_append_c(shard_info_json, '}');
	return g_string_free(shard_info_json, FALSE);
}

GError*
shard_info_decode_json(struct json_object *jshard_info,
		struct shard_info_s **pshard_info)
{
	EXTRA_ASSERT(jshard_info != NULL);
	EXTRA_ASSERT(pshard_info != NULL);

	GError *err = NULL;

	struct json_object *jroot_cid = NULL, *jtimestamp = NULL,
			*jlower = NULL, *jupper = NULL;
	struct oio_ext_json_mapping_s mapping[] = {
		{"root_cid",  &jroot_cid,       json_type_string, 1},
		{"timestamp", &jtimestamp, json_type_int,         1},
		{"lower",     &jlower,          json_type_string, 1},
		{"upper",     &jupper,          json_type_string, 1},
		{NULL,NULL,0,0}
	};
	err = oio_ext_extract_json(jshard_info, mapping);
	if (err) {
		return err;
	}

	struct shard_info_s *shard_info = g_malloc0(
			sizeof(struct shard_info_s));
	shard_info->root_cid = g_strdup(json_object_get_string(jroot_cid));
	shard_info->timestamp = json_object_get_int64(jtimestamp);
	shard_info->lower = g_strdup(json_object_get_string(jlower));
	shard_info->upper = g_strdup(json_object_get_string(jupper));
	*pshard_info = shard_info;

	return err;
}

GError*
shard_info_decode(const gchar *str, struct shard_info_s **pshard_info)
{
	EXTRA_ASSERT(str != NULL);
	EXTRA_ASSERT(pshard_info != NULL);

	GError *err = NULL;

	struct json_tokener *tok = json_tokener_new();
	struct json_object *jshard_info = json_tokener_parse_ex(tok,
			str, strlen(str));
	json_tokener_free(tok);
	if (!jshard_info) {
		err = BADREQ("Parse error");
		goto end;
	}

	err = shard_info_decode_json(jshard_info, pshard_info);

end:
	json_object_put(jshard_info);
	if (err) {
		g_prefix_error(&err, "Failed to decode shard info: ");
	}
	return err;
}

GError*
shard_info_check_range(struct shard_info_s *shard_info, const gchar *path)
{
	EXTRA_ASSERT(shard_info != NULL);

	if (_shard_check_range(shard_info->lower, shard_info->upper, path) != 0) {
		return BADREQ("Out of range: Not managed by this shard");
	}
	return NULL;
}

void
shard_info_free(struct shard_info_s *shard_info)
{
	if (!shard_info)
		return;

	g_free(shard_info->root_cid);
	g_free(shard_info->lower);
	g_free(shard_info->upper);
	g_free(shard_info);
}
