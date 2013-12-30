/*
 * Adobe HTTP Dynamic Streaming segmenter
 * Copyright (c) 2013, Ilya Murav'jov
 *
 * hdsenc.c is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * hdsenc.c is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with hdsenc.c; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

#include <float.h>

#include "libavutil/avassert.h"
#include "libavutil/mathematics.h"
#include "libavutil/parseutils.h"
#include "libavutil/avstring.h"
#include "libavutil/opt.h"
#include "libavutil/log.h"
#include "libavutil/timestamp.h"

#include "avformat.h"
#include "internal.h"

typedef struct ListEntry {
    char  name[1024];
    int   duration;
    struct ListEntry *next;
} ListEntry;

typedef struct HDSContext {
    const AVClass *class;  // Class for private options.
    unsigned int number;
    unsigned int start_fragment;
    AVOutputFormat *oformat;
    AVFormatContext *avf;
    float time;            // Set by a private option.
    int  size;             // Set by a private option.
    int  wrap;             // Set by a private option.
    int64_t recording_time;
    int has_video;
    int64_t start_pts;
    int64_t end_pts;
    int64_t duration;      ///< last segment duration computed so far, in seconds
    int nb_entries;
    ListEntry *list;
    ListEntry *end_list;
    char *basename;
    AVIOContext *pb;

    int is_first_pkt;      ///< tells if it is the first packet in the segment
} HDSContext;

static int hds_mux_init(AVFormatContext *s)
{
    HDSContext *hls = s->priv_data;
    AVFormatContext *oc;
    int i, av_unused err;

    //hls->avf = oc = avformat_alloc_context();
    err = avformat_alloc_output_context2(&oc, hls->oformat, NULL, NULL);
    if (!oc) {
        //print_error(s->filename, err);
        return AVERROR(ENOMEM);
    }
    //oc->oformat            = hls->oformat;

    hls->avf = oc;
    oc->interrupt_callback = s->interrupt_callback;

    for (i = 0; i < s->nb_streams; i++) {
        AVStream *st;
        AVCodecContext *ocodec;

        if (!(st = avformat_new_stream(oc, NULL)))
            return AVERROR(ENOMEM);

        ocodec = st->codec;
        avcodec_copy_context(ocodec, s->streams[i]->codec);
        st->sample_aspect_ratio = s->streams[i]->sample_aspect_ratio;
        // явно обнуляем, так как нам не важно, какой тег был на icodec,
        // главное - соответ. FLV-тег по codec_id
        ocodec->codec_tag = 0;
    }

    return 0;
}

static int append_entry(HDSContext *hls, uint64_t duration)
{
    ListEntry *en = av_malloc(sizeof(*en));

    if (!en)
        return AVERROR(ENOMEM);

    av_strlcpy(en->name, av_basename(hls->avf->filename), sizeof(en->name));

    en->duration = duration;
    en->next     = NULL;

    if (!hls->list)
        hls->list = en;
    else
        hls->end_list->next = en;

    hls->end_list = en;

    if (hls->nb_entries >= hls->size) {
        en = hls->list;
        hls->list = en->next;
        av_free(en);
    } else
        hls->nb_entries++;

    return 0;
}

static void free_entries(HDSContext *hls)
{
    ListEntry *p = hls->list, *en;

    while(p) {
        en = p;
        p = p->next;
        av_free(en);
    }
}

static int hds_window(AVFormatContext *s, int last)
{
    HDSContext *hls = s->priv_data;
    int ret = 0;

    // :TODO: generate abst file & make this configurable
    return ret;

    if ((ret = avio_open2(&hls->pb, s->filename, AVIO_FLAG_WRITE,
                          &s->interrupt_callback, NULL)) < 0)
        goto fail;

    // :TODO:

fail:
    avio_closep(&hls->pb);
    return ret;
}

static int hds_start(AVFormatContext *s)
{
    HDSContext *c = s->priv_data;
    AVFormatContext *oc = c->avf;
    int err = 0;

    if (av_get_frame_filename(oc->filename, sizeof(oc->filename),
                              c->basename, (c->wrap ? c->number % c->wrap : c->number) + 1) < 0) {
        av_log(oc, AV_LOG_ERROR, "Invalid segment filename template '%s'\n", c->basename);
        return AVERROR(EINVAL);
    }
    c->number++;

    if ((err = avio_open2(&oc->pb, oc->filename, AVIO_FLAG_WRITE,
                          &s->interrupt_callback, NULL)) < 0)
        return err;

    //if (oc->oformat->priv_class && oc->priv_data)
    //    av_opt_set(oc->priv_data, "flv_flags", "fragmented_output", 0);
    av_assert0(oc->oformat->priv_class && oc->priv_data);
    av_opt_set(oc->priv_data, "flv_flags", "fragmented_output", 0);

    c->is_first_pkt = 1;

    return 0;
}

static int hds_write_header(AVFormatContext *s)
{
    HDSContext *hls = s->priv_data;
    int ret, i;
    char *p;
    const char *pattern = "/Seg1-Frag%d";
    int basename_size = strlen(s->filename) + strlen(pattern) + 1;

    hls->number      = hls->start_fragment - 1;

    hls->recording_time = hls->time * AV_TIME_BASE;
    hls->start_pts      = AV_NOPTS_VALUE;

    for (i = 0; i < s->nb_streams; i++)
        hls->has_video +=
            s->streams[i]->codec->codec_type == AVMEDIA_TYPE_VIDEO;

    if (hls->has_video > 1)
        av_log(s, AV_LOG_WARNING,
               "More than a single video stream present, "
               "expect issues decoding it.\n");

    hls->oformat = av_guess_format("flv", NULL, NULL);

    if (!hls->oformat) {
        ret = AVERROR_MUXER_NOT_FOUND;
        goto fail;
    }

    hls->basename = av_malloc(basename_size);

    if (!hls->basename) {
        ret = AVERROR(ENOMEM);
        goto fail;
    }

    strcpy(hls->basename, s->filename);

    p = strrchr(hls->basename, '/');

    if (p)
        *p = '\0';

    av_strlcat(hls->basename, pattern, basename_size);

    if ((ret = hds_mux_init(s)) < 0)
        goto fail;

    if ((ret = hds_start(s)) < 0)
        goto fail;

    if ((ret = avformat_write_header(hls->avf, NULL)) < 0)
        return ret;

fail:
    if (ret) {
        av_free(hls->basename);
        if (hls->avf)
            avformat_free_context(hls->avf);
    }
    return ret;
}

// %.6g in (av_ts_make_time_string) makes 2019059.274200 as 2.019+e6
static inline char *av_ts_make_full_time_string(char *buf, int64_t ts, AVRational *tb)
{
    if (ts == AV_NOPTS_VALUE) snprintf(buf, AV_TS_MAX_STRING_SIZE, "NOPTS");
    else                      snprintf(buf, AV_TS_MAX_STRING_SIZE, "%.6f", av_q2d(*tb) * ts);
    return buf;
}

static int hds_write_packet(AVFormatContext *s, AVPacket *pkt)
{
    HDSContext *hls = s->priv_data;
    AVFormatContext *oc = hls->avf;
    AVStream *st = s->streams[pkt->stream_index];
    int64_t end_pts = hls->recording_time * hls->number;
    int is_ref_pkt = 1;
    int ret, can_split = 1;

    if (hls->start_pts == AV_NOPTS_VALUE) {
        hls->start_pts = pkt->pts;
        hls->end_pts   = pkt->pts;
    }

    if (hls->has_video) {
        can_split = st->codec->codec_type == AVMEDIA_TYPE_VIDEO &&
                    pkt->flags & AV_PKT_FLAG_KEY;
        is_ref_pkt = st->codec->codec_type == AVMEDIA_TYPE_VIDEO;
    }
    if (pkt->pts == AV_NOPTS_VALUE)
        is_ref_pkt = can_split = 0;

    if (is_ref_pkt)
        hls->duration = av_rescale(pkt->pts - hls->end_pts,
                                   st->time_base.num, st->time_base.den);

    if (can_split && av_compare_ts(pkt->pts - hls->start_pts, st->time_base,
                                   end_pts, AV_TIME_BASE_Q) >= 0) {
        ret = append_entry(hls, hls->duration);
        if (ret)
            return ret;

        hls->end_pts = pkt->pts;
        hls->duration = 0;

        // tell flv muxer to end mdat box
        av_write_frame(oc, NULL);
        avio_close(oc->pb);

        ret = hds_start(s);

        if (ret)
            return ret;

        oc = hls->avf;

        if ((ret = hds_window(s, 0)) < 0)
            return ret;
    }

    if (hls->is_first_pkt) {
        char time_buf[64];
        av_log(s, AV_LOG_WARNING, "hds:'%s' starts with packet stream:%d pts:%s pts_time:%s\n",
               hls->avf->filename, pkt->stream_index,
               av_ts2str(pkt->pts), av_ts_make_full_time_string(time_buf, pkt->pts, &st->time_base));
        hls->is_first_pkt = 0;
    }

    ret = ff_write_chained(oc, pkt->stream_index, pkt, s);

    return ret;
}

static int hds_write_trailer(struct AVFormatContext *s)
{
    HDSContext *hls = s->priv_data;
    AVFormatContext *oc = hls->avf;

    // :TRICKY: No trailer call(s) because we have no headers to update!
    //av_write_trailer(oc);
    avio_closep(&oc->pb);
    avformat_free_context(oc);
    av_free(hls->basename);
    append_entry(hls, hls->duration);
    hds_window(s, 1);

    free_entries(hls);
    avio_close(hls->pb);
    return 0;
}

#define OFFSET(x) offsetof(HDSContext, x)
#define E AV_OPT_FLAG_ENCODING_PARAM
static const AVOption options[] = {
    {"hds_start",     "set first fragment number",    OFFSET(start_fragment), AV_OPT_TYPE_INT,  {.i64 = 1},     0, INT64_MAX, E},
    {"hds_time",      "set segment length in seconds",           OFFSET(time),    AV_OPT_TYPE_FLOAT,  {.dbl = 2},     0, FLT_MAX, E},
    {"hds_list_size", "set maximum number of playlist entries",  OFFSET(size),    AV_OPT_TYPE_INT,    {.i64 = 5},     0, INT_MAX, E},
    {"hds_wrap",      "set number after which the index wraps",  OFFSET(wrap),    AV_OPT_TYPE_INT,    {.i64 = 0},     0, INT_MAX, E},
    { NULL },
};

static const AVClass hds_class = {
    .class_name = "hds muxer",
    .item_name  = av_default_item_name,
    .option     = options,
    .version    = LIBAVUTIL_VERSION_INT,
};

AVOutputFormat ff_hds_muxer = {
    .name           = "hds",
    .long_name      = NULL_IF_CONFIG_SMALL("Adobe HTTP Dynamic Streaming"),
    .extensions     = "f4m",
    .priv_data_size = sizeof(HDSContext),
    .audio_codec    = AV_CODEC_ID_AAC,
    .video_codec    = AV_CODEC_ID_H264,
    .flags          = AVFMT_NOFILE,
    .write_header   = hds_write_header,
    .write_packet   = hds_write_packet,
    .write_trailer  = hds_write_trailer,
    .priv_class     = &hds_class,
};
