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

#ifndef OIO_SDS__meta2v2__meta2_utils_sharding_h
# define OIO_SDS__meta2v2__meta2_utils_sharding_h 1

#include <glib.h>

#include <metautils/lib/metautils.h>

struct shard_range_s {
	guint index;
	gchar *lower;
	gchar *upper;
	gchar *cid;
};

typedef GTree* shard_ranges_t;

GError* shard_ranges_decode(const gchar *str, shard_ranges_t *pshard_ranges);

gchar* shard_ranges_encode(shard_ranges_t shard_ranges);

struct shard_range_s *shard_ranges_get_shard_range(
		shard_ranges_t shard_ranges, const gchar *path);

void shard_ranges_free(shard_ranges_t shard_ranges);

#endif /*OIO_SDS__meta2v2__meta2_utils_sharding_h*/
