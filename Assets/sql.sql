-- auto-generated definition
create table user
(
    uid         bigint unsigned auto_increment comment '用户ID'
        primary key,
    username    varchar(50)       not null comment '用户账号',
    password    varchar(255)      not null comment '用户密码',
    nickname    varchar(32)       not null comment '用户昵称',
    avatar      varchar(500)      null comment '用户头像url',
    background  varchar(500)      null comment '主页背景图url',
    gender      tinyint default 2 not null comment '性别 0女 1男 2未知',
    description varchar(100)      null comment '个性签名',
    exp         int     default 0 not null comment '经验值',
    coin        double  default 0 not null comment '硬币数',
    vip         tinyint default 0 not null comment '会员类型 0普通用户 1月度大会员 2季度大会员 3年度大会员',
    status      tinyint default 0 not null comment '状态 0正常 1封禁 2注销',
    role        tinyint default 0 not null comment '角色类型 0普通用户 1管理员 2超级管理员',
    auth        tinyint default 0 not null comment '官方认证 0普通用户 1个人认证 2机构认证',
    auth_msg    varchar(30)       null comment '认证说明',
    create_date datetime          not null comment '创建时间',
    delete_date datetime          null comment '注销时间',
    constraint nickname
        unique (nickname),
    constraint uid
        unique (uid),
    constraint username
        unique (username)
)
    comment '用户表' charset = utf8mb3;

-- auto-generated definition
create table video
(
    vid         bigint unsigned auto_increment comment '视频ID'
        primary key,
    uid         bigint unsigned   not null comment '投稿用户ID',
    title       varchar(80)       not null comment '标题',
    type        tinyint default 1 not null comment '类型 1自制 2转载',
    auth        tinyint default 0 not null comment '作者声明 0不声明 1未经允许禁止转载',
    duration    double  default 0 not null comment '播放总时长 单位秒',
    mc_id       varchar(20)       not null comment '主分区ID',
    sc_id       varchar(20)       not null comment '子分区ID',
    tags        varchar(500)      null comment '标签，不同标签之间用空格分隔',
    descr       varchar(2000)     null comment '简介',
    cover_url   varchar(500)      not null comment '封面url',
    video_url   varchar(500)      not null comment '视频url',
    status      tinyint default 0 not null comment '状态 0审核中 1已过审 2未通过 3已删除',
    upload_date datetime          not null comment '上传时间',
    delete_date datetime          null comment '删除时间',
    visible     tinyint default 0 null comment '可见范围：0公开，1仅自己可见',
    constraint vid
        unique (vid)
)
    comment '视频表' charset = utf8mb3;

-- auto-generated definition
create table danmu
(
    id          int auto_increment comment '弹幕ID'
        primary key,
    vid         bigint unsigned              not null comment '视频ID',
    uid         bigint unsigned              not null comment '用户ID',
    content     varchar(100)                 not null comment '弹幕内容',
    fontsize    tinyint    default 25        not null comment '字体大小',
    mode        tinyint    default 1         not null comment '弹幕模式 0滚动 1顶部 2底部',
    color       varchar(7) default '#FFFFFF' not null comment '弹幕颜色 6位十六进制标准格式',
    time_point  double                       not null comment '弹幕所在视频的时间点',
    status      tinyint    default 1         not null comment '弹幕状态 1默认过审 2被举报审核中 3删除',
    create_date datetime                     not null comment '发送弹幕的日期时间',
    constraint id
        unique (id)
)
    comment '弹幕表';

-- auto-generated definition
create table user_tag
(
    id           bigint auto_increment comment '主键ID'
        primary key,
    uid          bigint unsigned                     not null comment '用户ID',
    tag_name     varchar(50)                         not null comment '语义标签名，如“二次元”',
    weight       float     default 0                 not null comment '标签权重',
    version      int       default 1                 null comment '版本号（用于乐观锁）',
    created_date timestamp default CURRENT_TIMESTAMP null comment '首次创建时间',
    updated_date timestamp default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP comment '最后更新时间',
    constraint uk_uid_tag
        unique (uid, tag_name) comment '每个用户对每个标签只有一条记录'
)
    comment '用户语义标签表（用于用户画像）';

create index idx_tag
    on user_tag (tag_name)
    comment '按标签查询加速';

create index idx_uid
    on user_tag (uid)
    comment '按用户查询加速';

create index uid_name_weight
    on user_tag (uid asc, tag_name asc, weight desc)
    comment 'uid&tag_name&weight联合索引';

-- auto-generated definition
create table user_video
(
    id           int auto_increment comment '唯一标识'
        primary key,
    uid          bigint unsigned   not null comment '观看视频的用户UID',
    vid          bigint unsigned   not null comment '视频ID',
    play         int     default 0 not null comment '播放次数',
    love         tinyint default 0 not null comment '点赞 0没赞 1已点赞',
    unlove       tinyint default 0 not null comment '是否点踩：0-是；1-否',
    coin         tinyint default 0 not null comment '投币数，默认0，最高为2',
    collect      tinyint default 0 not null comment '是否收藏：1-已收藏；2-未收藏',
    play_time    datetime          not null comment '最近播放时间',
    love_time    datetime          null comment '最近点赞时间',
    coin_time    datetime          null comment '最近投币时间',
    collect_time datetime          null comment '最近收藏时间',
    constraint id
        unique (id),
    constraint uid_vid__index
        unique (uid, vid)
)
    comment '用户视频关联表';

