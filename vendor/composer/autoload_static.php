<?php

// autoload_static.php @generated by Composer

namespace Composer\Autoload;

class ComposerStaticInit55a96604e16de774956cff9829e3b844
{
    public static $files = array (
        '3a37ebac017bc098e9a86b35401e7a68' => __DIR__ . '/..' . '/mongodb/mongodb/src/functions.php',
    );

    public static $prefixLengthsPsr4 = array (
        'M' => 
        array (
            'MongoDB\\' => 8,
        ),
    );

    public static $prefixDirsPsr4 = array (
        'MongoDB\\' => 
        array (
            0 => __DIR__ . '/..' . '/mongodb/mongodb/src',
        ),
    );

    public static $prefixesPsr0 = array (
        'S' => 
        array (
            'Stunti' => 
            array (
                0 => __DIR__ . '/../..' . '/library',
            ),
        ),
    );

    public static function getInitializer(ClassLoader $loader)
    {
        return \Closure::bind(function () use ($loader) {
            $loader->prefixLengthsPsr4 = ComposerStaticInit55a96604e16de774956cff9829e3b844::$prefixLengthsPsr4;
            $loader->prefixDirsPsr4 = ComposerStaticInit55a96604e16de774956cff9829e3b844::$prefixDirsPsr4;
            $loader->prefixesPsr0 = ComposerStaticInit55a96604e16de774956cff9829e3b844::$prefixesPsr0;

        }, null, ClassLoader::class);
    }
}